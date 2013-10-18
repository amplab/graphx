/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.spark.deploy.client

import java.util.concurrent.TimeoutException

import akka.actor._
import akka.actor.Terminated
import akka.pattern.ask
import akka.util.Duration
import akka.util.duration._
import akka.remote.RemoteClientDisconnected
import akka.remote.RemoteClientLifeCycleEvent
import akka.remote.RemoteClientShutdown
import akka.dispatch.Await

import org.apache.spark.Logging
import org.apache.spark.deploy.{ApplicationDescription, ExecutorState}
import org.apache.spark.deploy.DeployMessages._
import org.apache.spark.deploy.master.Master


/**
 * The main class used to talk to a Spark deploy cluster. Takes a master URL, an app description,
 * and a listener for cluster events, and calls back the listener when various events occur.
 *
 * @param masterUrls Each url should look like spark://host:port.
 */
private[spark] class Client(
    actorSystem: ActorSystem,
    masterUrls: Array[String],
    appDescription: ApplicationDescription,
    listener: ClientListener)
  extends Logging {

  val REGISTRATION_TIMEOUT = 20.seconds
  val REGISTRATION_RETRIES = 3

  var actor: ActorRef = null
  var appId: String = null
  var registered = false
  var activeMasterUrl: String = null

  class ClientActor extends Actor with Logging {
    var master: ActorRef = null
    var masterAddress: Address = null
    var alreadyDisconnected = false  // To avoid calling listener.disconnected() multiple times
    var alreadyDead = false  // To avoid calling listener.dead() multiple times

    override def preStart() {
      try {
        registerWithMaster()
      } catch {
        case e: Exception =>
          logWarning("Failed to connect to master", e)
          markDisconnected()
          context.stop(self)
      }
    }

    def tryRegisterAllMasters() {
      for (masterUrl <- masterUrls) {
        logInfo("Connecting to master " + masterUrl + "...")
        val actor = context.actorFor(Master.toAkkaUrl(masterUrl))
        actor ! RegisterApplication(appDescription)
      }
    }

    def registerWithMaster() {
      tryRegisterAllMasters()

      var retries = 0
      lazy val retryTimer: Cancellable =
        context.system.scheduler.schedule(REGISTRATION_TIMEOUT, REGISTRATION_TIMEOUT) {
          retries += 1
          if (registered) {
            retryTimer.cancel()
          } else if (retries >= REGISTRATION_RETRIES) {
            logError("All masters are unresponsive! Giving up.")
            markDead()
          } else {
            tryRegisterAllMasters()
          }
        }
      retryTimer // start timer
    }

    def changeMaster(url: String) {
      activeMasterUrl = url
      master = context.actorFor(Master.toAkkaUrl(url))
      masterAddress = master.path.address
      context.system.eventStream.subscribe(self, classOf[RemoteClientLifeCycleEvent])
      context.watch(master)  // Doesn't work with remote actors, but useful for testing
    }

    override def receive = {
      case RegisteredApplication(appId_, masterUrl) =>
        appId = appId_
        registered = true
        changeMaster(masterUrl)
        listener.connected(appId)

      case ApplicationRemoved(message) =>
        logError("Master removed our application: %s; stopping client".format(message))
        markDisconnected()
        context.stop(self)

      case ExecutorAdded(id: Int, workerId: String, hostPort: String, cores: Int, memory: Int) =>
        val fullId = appId + "/" + id
        logInfo("Executor added: %s on %s (%s) with %d cores".format(fullId, workerId, hostPort, cores))
        listener.executorAdded(fullId, workerId, hostPort, cores, memory)

      case ExecutorUpdated(id, state, message, exitStatus) =>
        val fullId = appId + "/" + id
        val messageText = message.map(s => " (" + s + ")").getOrElse("")
        logInfo("Executor updated: %s is now %s%s".format(fullId, state, messageText))
        if (ExecutorState.isFinished(state)) {
          listener.executorRemoved(fullId, message.getOrElse(""), exitStatus)
        }

      case MasterChanged(masterUrl, masterWebUiUrl) =>
        logInfo("Master has changed, new master is at " + masterUrl)
        context.unwatch(master)
        changeMaster(masterUrl)
        alreadyDisconnected = false
        sender ! MasterChangeAcknowledged(appId)

      case Terminated(actor_) if actor_ == master =>
        logWarning("Connection to master failed; waiting for master to reconnect...")
        markDisconnected()

      case RemoteClientDisconnected(transport, address) if address == masterAddress =>
        logWarning("Connection to master failed; waiting for master to reconnect...")
        markDisconnected()

      case RemoteClientShutdown(transport, address) if address == masterAddress =>
        logWarning("Connection to master failed; waiting for master to reconnect...")
        markDisconnected()

      case StopClient =>
        markDead()
        sender ! true
        context.stop(self)
    }

    /**
     * Notify the listener that we disconnected, if we hadn't already done so before.
     */
    def markDisconnected() {
      if (!alreadyDisconnected) {
        listener.disconnected()
        alreadyDisconnected = true
      }
    }

    def markDead() {
      if (!alreadyDead) {
        listener.dead()
        alreadyDead = true
      }
    }
  }

  def start() {
    // Just launch an actor; it will call back into the listener.
    actor = actorSystem.actorOf(Props(new ClientActor))
  }

  def stop() {
    if (actor != null) {
      try {
        val timeout = Duration.create(System.getProperty("spark.akka.askTimeout", "10").toLong, "seconds")
        val future = actor.ask(StopClient)(timeout)
        Await.result(future, timeout)
      } catch {
        case e: TimeoutException =>
          logInfo("Stop request to Master timed out; it may already be shut down.")
      }
      actor = null
    }
  }
}
