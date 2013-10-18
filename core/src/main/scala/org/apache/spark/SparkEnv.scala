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

package org.apache.spark

import collection.mutable
import serializer.Serializer

import akka.actor.{Actor, ActorRef, Props, ActorSystemImpl, ActorSystem}
import akka.remote.RemoteActorRefProvider

import org.apache.spark.broadcast.BroadcastManager
import org.apache.spark.metrics.MetricsSystem
import org.apache.spark.deploy.SparkHadoopUtil
import org.apache.spark.storage.{BlockManagerMasterActor, BlockManager, BlockManagerMaster}
import org.apache.spark.network.ConnectionManager
import org.apache.spark.serializer.{Serializer, SerializerManager}
import org.apache.spark.util.{Utils, AkkaUtils}
import org.apache.spark.api.python.PythonWorkerFactory


/**
 * Holds all the runtime environment objects for a running Spark instance (either master or worker),
 * including the serializer, Akka actor system, block manager, map output tracker, etc. Currently
 * Spark code finds the SparkEnv through a thread-local variable, so each thread that accesses these
 * objects needs to have the right SparkEnv set. You can get the current environment with
 * SparkEnv.get (e.g. after creating a SparkContext) and set it with SparkEnv.set.
 */
class SparkEnv (
    val executorId: String,
    val actorSystem: ActorSystem,
    val serializerManager: SerializerManager,
    val serializer: Serializer,
    val closureSerializer: Serializer,
    val cacheManager: CacheManager,
    val mapOutputTracker: MapOutputTracker,
    val shuffleFetcher: ShuffleFetcher,
    val broadcastManager: BroadcastManager,
    val blockManager: BlockManager,
    val connectionManager: ConnectionManager,
    val httpFileServer: HttpFileServer,
    val sparkFilesDir: String,
    val metricsSystem: MetricsSystem) {

  private val pythonWorkers = mutable.HashMap[(String, Map[String, String]), PythonWorkerFactory]()

  val hadoop = {
    val yarnMode = java.lang.Boolean.valueOf(System.getProperty("SPARK_YARN_MODE", System.getenv("SPARK_YARN_MODE")))
    if(yarnMode) {
      try {
        Class.forName("org.apache.spark.deploy.yarn.YarnSparkHadoopUtil").newInstance.asInstanceOf[SparkHadoopUtil]
      } catch {
        case th: Throwable => throw new SparkException("Unable to load YARN support", th)
      }
    } else {
      new SparkHadoopUtil
    }
  }

  def stop() {
    pythonWorkers.foreach { case(key, worker) => worker.stop() }
    httpFileServer.stop()
    mapOutputTracker.stop()
    shuffleFetcher.stop()
    broadcastManager.stop()
    blockManager.stop()
    blockManager.master.stop()
    metricsSystem.stop()
    actorSystem.shutdown()
    // Unfortunately Akka's awaitTermination doesn't actually wait for the Netty server to shut
    // down, but let's call it anyway in case it gets fixed in a later release
    actorSystem.awaitTermination()
  }

  def createPythonWorker(pythonExec: String, envVars: Map[String, String]): java.net.Socket = {
    synchronized {
      val key = (pythonExec, envVars)
      pythonWorkers.getOrElseUpdate(key, new PythonWorkerFactory(pythonExec, envVars)).create()
    }
  }
}

object SparkEnv extends Logging {
  private val env = new ThreadLocal[SparkEnv]
  @volatile private var lastSetSparkEnv : SparkEnv = _

  def set(e: SparkEnv) {
	  lastSetSparkEnv = e
    env.set(e)
  }

  /**
   * Returns the ThreadLocal SparkEnv, if non-null. Else returns the SparkEnv
   * previously set in any thread.
   */
  def get: SparkEnv = {
    Option(env.get()).getOrElse(lastSetSparkEnv)
  }

  /**
   * Returns the ThreadLocal SparkEnv.
   */
  def getThreadLocal : SparkEnv = {
	  env.get()
  }

  def createFromSystemProperties(
      executorId: String,
      hostname: String,
      port: Int,
      isDriver: Boolean,
      isLocal: Boolean): SparkEnv = {

    val (actorSystem, boundPort) = AkkaUtils.createActorSystem("spark", hostname, port)

    // Bit of a hack: If this is the driver and our port was 0 (meaning bind to any free port),
    // figure out which port number Akka actually bound to and set spark.driver.port to it.
    if (isDriver && port == 0) {
      System.setProperty("spark.driver.port", boundPort.toString)
    }

    // set only if unset until now.
    if (System.getProperty("spark.hostPort", null) == null) {
      if (!isDriver){
        // unexpected
        Utils.logErrorWithStack("Unexpected NOT to have spark.hostPort set")
      }
      Utils.checkHost(hostname)
      System.setProperty("spark.hostPort", hostname + ":" + boundPort)
    }

    val classLoader = Thread.currentThread.getContextClassLoader

    // Create an instance of the class named by the given Java system property, or by
    // defaultClassName if the property is not set, and return it as a T
    def instantiateClass[T](propertyName: String, defaultClassName: String): T = {
      val name = System.getProperty(propertyName, defaultClassName)
      Class.forName(name, true, classLoader).newInstance().asInstanceOf[T]
    }

    val serializerManager = new SerializerManager

    val serializer = serializerManager.setDefault(
      System.getProperty("spark.serializer", "org.apache.spark.serializer.JavaSerializer"))
    logInfo("spark.serializer is " + System.getProperty("spark.serializer"))

    val closureSerializer = serializerManager.get(
      System.getProperty("spark.closure.serializer", "org.apache.spark.serializer.JavaSerializer"))

    def registerOrLookup(name: String, newActor: => Actor): ActorRef = {
      if (isDriver) {
        logInfo("Registering " + name)
        actorSystem.actorOf(Props(newActor), name = name)
      } else {
        val driverHost: String = System.getProperty("spark.driver.host", "localhost")
        val driverPort: Int = System.getProperty("spark.driver.port", "7077").toInt
        Utils.checkHost(driverHost, "Expected hostname")
        val url = "akka://spark@%s:%s/user/%s".format(driverHost, driverPort, name)
        logInfo("Connecting to " + name + ": " + url)
        actorSystem.actorFor(url)
      }
    }

    val blockManagerMaster = new BlockManagerMaster(registerOrLookup(
      "BlockManagerMaster",
      new BlockManagerMasterActor(isLocal)))
    val blockManager = new BlockManager(executorId, actorSystem, blockManagerMaster, serializer)

    val connectionManager = blockManager.connectionManager

    val broadcastManager = new BroadcastManager(isDriver)

    val cacheManager = new CacheManager(blockManager)

    // Have to assign trackerActor after initialization as MapOutputTrackerActor
    // requires the MapOutputTracker itself
    val mapOutputTracker = new MapOutputTracker()
    mapOutputTracker.trackerActor = registerOrLookup(
      "MapOutputTracker",
      new MapOutputTrackerActor(mapOutputTracker))

    val shuffleFetcher = instantiateClass[ShuffleFetcher](
      "spark.shuffle.fetcher", "org.apache.spark.BlockStoreShuffleFetcher")

    val httpFileServer = new HttpFileServer()
    httpFileServer.initialize()
    System.setProperty("spark.fileserver.uri", httpFileServer.serverUri)

    val metricsSystem = if (isDriver) {
      MetricsSystem.createMetricsSystem("driver")
    } else {
      MetricsSystem.createMetricsSystem("executor")
    }
    metricsSystem.start()

    // Set the sparkFiles directory, used when downloading dependencies.  In local mode,
    // this is a temporary directory; in distributed mode, this is the executor's current working
    // directory.
    val sparkFilesDir: String = if (isDriver) {
      Utils.createTempDir().getAbsolutePath
    } else {
      "."
    }

    // Warn about deprecated spark.cache.class property
    if (System.getProperty("spark.cache.class") != null) {
      logWarning("The spark.cache.class property is no longer being used! Specify storage " +
        "levels using the RDD.persist() method instead.")
    }

    new SparkEnv(
      executorId,
      actorSystem,
      serializerManager,
      serializer,
      closureSerializer,
      cacheManager,
      mapOutputTracker,
      shuffleFetcher,
      broadcastManager,
      blockManager,
      connectionManager,
      httpFileServer,
      sparkFilesDir,
      metricsSystem)
  }
}
