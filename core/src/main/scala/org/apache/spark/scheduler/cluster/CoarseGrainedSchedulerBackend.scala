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

package org.apache.spark.scheduler.cluster

import java.util.concurrent.atomic.AtomicInteger

import scala.collection.mutable.{ArrayBuffer, HashMap, HashSet}

import akka.actor._
import akka.dispatch.Await
import akka.pattern.ask
import akka.remote.{RemoteClientShutdown, RemoteClientDisconnected, RemoteClientLifeCycleEvent}
import akka.util.Duration
import akka.util.duration._

import org.apache.spark.{SparkException, Logging, TaskState}
import org.apache.spark.scheduler.TaskDescription
import org.apache.spark.scheduler.cluster.CoarseGrainedClusterMessages._
import org.apache.spark.util.Utils

/**
 * A scheduler backend that waits for coarse grained executors to connect to it through Akka.
 * This backend holds onto each executor for the duration of the Spark job rather than relinquishing
 * executors whenever a task is done and asking the scheduler to launch a new executor for
 * each new task. Executors may be launched in a variety of ways, such as Mesos tasks for the
 * coarse-grained Mesos mode or standalone processes for Spark's standalone deploy mode
 * (spark.deploy.*).
 */
private[spark]
class CoarseGrainedSchedulerBackend(scheduler: ClusterScheduler, actorSystem: ActorSystem)
  extends SchedulerBackend with Logging
{
  // Use an atomic variable to track total number of cores in the cluster for simplicity and speed
  var totalCoreCount = new AtomicInteger(0)

  class DriverActor(sparkProperties: Seq[(String, String)]) extends Actor {
    private val executorActor = new HashMap[String, ActorRef]
    private val executorAddress = new HashMap[String, Address]
    private val executorHost = new HashMap[String, String]
    private val freeCores = new HashMap[String, Int]
    private val actorToExecutorId = new HashMap[ActorRef, String]
    private val addressToExecutorId = new HashMap[Address, String]

    override def preStart() {
      // Listen for remote client disconnection events, since they don't go through Akka's watch()
      context.system.eventStream.subscribe(self, classOf[RemoteClientLifeCycleEvent])

      // Periodically revive offers to allow delay scheduling to work
      val reviveInterval = System.getProperty("spark.scheduler.revive.interval", "1000").toLong
      context.system.scheduler.schedule(0.millis, reviveInterval.millis, self, ReviveOffers)
    }

    def receive = {
      case RegisterExecutor(executorId, hostPort, cores) =>
        Utils.checkHostPort(hostPort, "Host port expected " + hostPort)
        if (executorActor.contains(executorId)) {
          sender ! RegisterExecutorFailed("Duplicate executor ID: " + executorId)
        } else {
          logInfo("Registered executor: " + sender + " with ID " + executorId)
          sender ! RegisteredExecutor(sparkProperties)
          context.watch(sender)
          executorActor(executorId) = sender
          executorHost(executorId) = Utils.parseHostPort(hostPort)._1
          freeCores(executorId) = cores
          executorAddress(executorId) = sender.path.address
          actorToExecutorId(sender) = executorId
          addressToExecutorId(sender.path.address) = executorId
          totalCoreCount.addAndGet(cores)
          makeOffers()
        }

      case StatusUpdate(executorId, taskId, state, data) =>
        scheduler.statusUpdate(taskId, state, data.value)
        if (TaskState.isFinished(state)) {
          freeCores(executorId) += 1
          makeOffers(executorId)
        }

      case ReviveOffers =>
        makeOffers()

      case KillTask(taskId, executorId) =>
        executorActor(executorId) ! KillTask(taskId, executorId)

      case StopDriver =>
        sender ! true
        context.stop(self)

      case RemoveExecutor(executorId, reason) =>
        removeExecutor(executorId, reason)
        sender ! true

      case Terminated(actor) =>
        actorToExecutorId.get(actor).foreach(removeExecutor(_, "Akka actor terminated"))

      case RemoteClientDisconnected(transport, address) =>
        addressToExecutorId.get(address).foreach(removeExecutor(_, "remote Akka client disconnected"))

      case RemoteClientShutdown(transport, address) =>
        addressToExecutorId.get(address).foreach(removeExecutor(_, "remote Akka client shutdown"))
    }

    // Make fake resource offers on all executors
    def makeOffers() {
      launchTasks(scheduler.resourceOffers(
        executorHost.toArray.map {case (id, host) => new WorkerOffer(id, host, freeCores(id))}))
    }

    // Make fake resource offers on just one executor
    def makeOffers(executorId: String) {
      launchTasks(scheduler.resourceOffers(
        Seq(new WorkerOffer(executorId, executorHost(executorId), freeCores(executorId)))))
    }

    // Launch tasks returned by a set of resource offers
    def launchTasks(tasks: Seq[Seq[TaskDescription]]) {
      for (task <- tasks.flatten) {
        freeCores(task.executorId) -= 1
        executorActor(task.executorId) ! LaunchTask(task)
      }
    }

    // Remove a disconnected slave from the cluster
    def removeExecutor(executorId: String, reason: String) {
      if (executorActor.contains(executorId)) {
        logInfo("Executor " + executorId + " disconnected, so removing it")
        val numCores = freeCores(executorId)
        actorToExecutorId -= executorActor(executorId)
        addressToExecutorId -= executorAddress(executorId)
        executorActor -= executorId
        executorHost -= executorId
        freeCores -= executorId
        totalCoreCount.addAndGet(-numCores)
        scheduler.executorLost(executorId, SlaveLost(reason))
      }
    }
  }

  var driverActor: ActorRef = null
  val taskIdsOnSlave = new HashMap[String, HashSet[String]]

  override def start() {
    val properties = new ArrayBuffer[(String, String)]
    val iterator = System.getProperties.entrySet.iterator
    while (iterator.hasNext) {
      val entry = iterator.next
      val (key, value) = (entry.getKey.toString, entry.getValue.toString)
      if (key.startsWith("spark.") && !key.equals("spark.hostPort")) {
        properties += ((key, value))
      }
    }
    driverActor = actorSystem.actorOf(
      Props(new DriverActor(properties)), name = CoarseGrainedSchedulerBackend.ACTOR_NAME)
  }

  private val timeout = Duration.create(System.getProperty("spark.akka.askTimeout", "10").toLong, "seconds")

  override def stop() {
    try {
      if (driverActor != null) {
        val future = driverActor.ask(StopDriver)(timeout)
        Await.result(future, timeout)
      }
    } catch {
      case e: Exception =>
        throw new SparkException("Error stopping standalone scheduler's driver actor", e)
    }
  }

  override def reviveOffers() {
    driverActor ! ReviveOffers
  }

  override def killTask(taskId: Long, executorId: String) {
    driverActor ! KillTask(taskId, executorId)
  }

  override def defaultParallelism() = Option(System.getProperty("spark.default.parallelism"))
      .map(_.toInt).getOrElse(math.max(totalCoreCount.get(), 2))

  // Called by subclasses when notified of a lost worker
  def removeExecutor(executorId: String, reason: String) {
    try {
      val future = driverActor.ask(RemoveExecutor(executorId, reason))(timeout)
      Await.result(future, timeout)
    } catch {
      case e: Exception =>
        throw new SparkException("Error notifying standalone scheduler's driver actor", e)
    }
  }
}

private[spark] object CoarseGrainedSchedulerBackend {
  val ACTOR_NAME = "CoarseGrainedScheduler"
}
