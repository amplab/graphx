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

package org.apache.spark.deploy.worker

import java.text.SimpleDateFormat
import java.util.Date
import java.io.File

import scala.collection.mutable.HashMap

import akka.actor._
import akka.remote.{RemoteClientLifeCycleEvent, RemoteClientShutdown, RemoteClientDisconnected}
import akka.util.duration._

import org.apache.spark.Logging
import org.apache.spark.deploy.{ExecutorDescription, ExecutorState}
import org.apache.spark.deploy.DeployMessages._
import org.apache.spark.deploy.master.Master
import org.apache.spark.deploy.worker.ui.WorkerWebUI
import org.apache.spark.metrics.MetricsSystem
import org.apache.spark.util.{Utils, AkkaUtils}

/**
  * @param masterUrls Each url should look like spark://host:port.
  */
private[spark] class Worker(
    host: String,
    port: Int,
    webUiPort: Int,
    cores: Int,
    memory: Int,
    masterUrls: Array[String],
    workDirPath: String = null)
  extends Actor with Logging {

  Utils.checkHost(host, "Expected hostname")
  assert (port > 0)

  val DATE_FORMAT = new SimpleDateFormat("yyyyMMddHHmmss")  // For worker and executor IDs

  // Send a heartbeat every (heartbeat timeout) / 4 milliseconds
  val HEARTBEAT_MILLIS = System.getProperty("spark.worker.timeout", "60").toLong * 1000 / 4

  val REGISTRATION_TIMEOUT = 20.seconds
  val REGISTRATION_RETRIES = 3

  // Index into masterUrls that we're currently trying to register with.
  var masterIndex = 0

  val masterLock: Object = new Object()
  var master: ActorRef = null
  var activeMasterUrl: String = ""
  var activeMasterWebUiUrl : String = ""
  @volatile var registered = false
  @volatile var connected = false
  val workerId = generateWorkerId()
  var sparkHome: File = null
  var workDir: File = null
  val executors = new HashMap[String, ExecutorRunner]
  val finishedExecutors = new HashMap[String, ExecutorRunner]
  val publicAddress = {
    val envVar = System.getenv("SPARK_PUBLIC_DNS")
    if (envVar != null) envVar else host
  }
  var webUi: WorkerWebUI = null

  var coresUsed = 0
  var memoryUsed = 0

  val metricsSystem = MetricsSystem.createMetricsSystem("worker")
  val workerSource = new WorkerSource(this)

  def coresFree: Int = cores - coresUsed
  def memoryFree: Int = memory - memoryUsed

  def createWorkDir() {
    workDir = Option(workDirPath).map(new File(_)).getOrElse(new File(sparkHome, "work"))
    try {
      // This sporadically fails - not sure why ... !workDir.exists() && !workDir.mkdirs()
      // So attempting to create and then check if directory was created or not.
      workDir.mkdirs()
      if ( !workDir.exists() || !workDir.isDirectory) {
        logError("Failed to create work directory " + workDir)
        System.exit(1)
      }
      assert (workDir.isDirectory)
    } catch {
      case e: Exception =>
        logError("Failed to create work directory " + workDir, e)
        System.exit(1)
    }
  }

  override def preStart() {
    assert(!registered)
    logInfo("Starting Spark worker %s:%d with %d cores, %s RAM".format(
      host, port, cores, Utils.megabytesToString(memory)))
    sparkHome = new File(Option(System.getenv("SPARK_HOME")).getOrElse("."))
    logInfo("Spark home: " + sparkHome)
    createWorkDir()
    webUi = new WorkerWebUI(this, workDir, Some(webUiPort))

    webUi.start()
    registerWithMaster()

    metricsSystem.registerSource(workerSource)
    metricsSystem.start()
  }

  def changeMaster(url: String, uiUrl: String) {
    masterLock.synchronized {
      activeMasterUrl = url
      activeMasterWebUiUrl = uiUrl
      master = context.actorFor(Master.toAkkaUrl(activeMasterUrl))
      context.system.eventStream.subscribe(self, classOf[RemoteClientLifeCycleEvent])
      context.watch(master) // Doesn't work with remote actors, but useful for testing
      connected = true
    }
  }

  def tryRegisterAllMasters() {
    for (masterUrl <- masterUrls) {
      logInfo("Connecting to master " + masterUrl + "...")
      val actor = context.actorFor(Master.toAkkaUrl(masterUrl))
      actor ! RegisterWorker(workerId, host, port, cores, memory, webUi.boundPort.get,
        publicAddress)
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
          System.exit(1)
        } else {
          tryRegisterAllMasters()
        }
      }
    retryTimer // start timer
  }

  override def receive = {
    case RegisteredWorker(masterUrl, masterWebUiUrl) =>
      logInfo("Successfully registered with master " + masterUrl)
      registered = true
      changeMaster(masterUrl, masterWebUiUrl)
      context.system.scheduler.schedule(0 millis, HEARTBEAT_MILLIS millis, self, SendHeartbeat)

    case SendHeartbeat =>
      masterLock.synchronized {
        if (connected) { master ! Heartbeat(workerId) }
      }

    case MasterChanged(masterUrl, masterWebUiUrl) =>
      logInfo("Master has changed, new master is at " + masterUrl)
      context.unwatch(master)
      changeMaster(masterUrl, masterWebUiUrl)

      val execs = executors.values.
        map(e => new ExecutorDescription(e.appId, e.execId, e.cores, e.state))
      sender ! WorkerSchedulerStateResponse(workerId, execs.toList)

    case RegisterWorkerFailed(message) =>
      if (!registered) {
        logError("Worker registration failed: " + message)
        System.exit(1)
      }

    case LaunchExecutor(masterUrl, appId, execId, appDesc, cores_, memory_, execSparkHome_) =>
      if (masterUrl != activeMasterUrl) {
        logWarning("Invalid Master (" + masterUrl + ") attempted to launch executor.")
      } else {
        logInfo("Asked to launch executor %s/%d for %s".format(appId, execId, appDesc.name))
        val manager = new ExecutorRunner(appId, execId, appDesc, cores_, memory_,
          self, workerId, host, new File(execSparkHome_), workDir, ExecutorState.RUNNING)
        executors(appId + "/" + execId) = manager
        manager.start()
        coresUsed += cores_
        memoryUsed += memory_
        masterLock.synchronized {
          master ! ExecutorStateChanged(appId, execId, manager.state, None, None)
        }
      }

    case ExecutorStateChanged(appId, execId, state, message, exitStatus) =>
      masterLock.synchronized {
        master ! ExecutorStateChanged(appId, execId, state, message, exitStatus)
      }
      val fullId = appId + "/" + execId
      if (ExecutorState.isFinished(state)) {
        val executor = executors(fullId)
        logInfo("Executor " + fullId + " finished with state " + state +
          message.map(" message " + _).getOrElse("") +
          exitStatus.map(" exitStatus " + _).getOrElse(""))
        finishedExecutors(fullId) = executor
        executors -= fullId
        coresUsed -= executor.cores
        memoryUsed -= executor.memory
      }

    case KillExecutor(masterUrl, appId, execId) =>
      if (masterUrl != activeMasterUrl) {
        logWarning("Invalid Master (" + masterUrl + ") attempted to launch executor " + execId)
      } else {
        val fullId = appId + "/" + execId
        executors.get(fullId) match {
          case Some(executor) =>
            logInfo("Asked to kill executor " + fullId)
            executor.kill()
          case None =>
            logInfo("Asked to kill unknown executor " + fullId)
        }
      }

    case Terminated(actor_) if actor_ == master =>
      masterDisconnected()

    case RemoteClientDisconnected(transport, address) if address == master.path.address =>
      masterDisconnected()

    case RemoteClientShutdown(transport, address) if address == master.path.address =>
      masterDisconnected()

    case RequestWorkerState => {
      sender ! WorkerStateResponse(host, port, workerId, executors.values.toList,
        finishedExecutors.values.toList, activeMasterUrl, cores, memory,
        coresUsed, memoryUsed, activeMasterWebUiUrl)
    }
  }

  def masterDisconnected() {
    logError("Connection to master failed! Waiting for master to reconnect...")
    connected = false
  }

  def generateWorkerId(): String = {
    "worker-%s-%s-%d".format(DATE_FORMAT.format(new Date), host, port)
  }

  override def postStop() {
    executors.values.foreach(_.kill())
    webUi.stop()
    metricsSystem.stop()
  }
}

private[spark] object Worker {
  def main(argStrings: Array[String]) {
    val args = new WorkerArguments(argStrings)
    val (actorSystem, _) = startSystemAndActor(args.host, args.port, args.webUiPort, args.cores,
      args.memory, args.masters, args.workDir)
    actorSystem.awaitTermination()
  }

  def startSystemAndActor(host: String, port: Int, webUiPort: Int, cores: Int, memory: Int,
    masterUrls: Array[String], workDir: String, workerNumber: Option[Int] = None)
    : (ActorSystem, Int) = {
    // The LocalSparkCluster runs multiple local sparkWorkerX actor systems
    val systemName = "sparkWorker" + workerNumber.map(_.toString).getOrElse("")
    val (actorSystem, boundPort) = AkkaUtils.createActorSystem(systemName, host, port)
    val actor = actorSystem.actorOf(Props(new Worker(host, boundPort, webUiPort, cores, memory,
      masterUrls, workDir)), name = "Worker")
    (actorSystem, boundPort)
  }

}
