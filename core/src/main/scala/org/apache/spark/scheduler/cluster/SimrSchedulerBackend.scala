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

import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.{Path, FileSystem}

import org.apache.spark.{Logging, SparkContext}
import org.apache.spark.scheduler.TaskSchedulerImpl

private[spark] class SimrSchedulerBackend(
    scheduler: TaskSchedulerImpl,
    sc: SparkContext,
    driverFilePath: String)
  extends CoarseGrainedSchedulerBackend(scheduler, sc.env.actorSystem)
  with Logging {

  val tmpPath = new Path(driverFilePath + "_tmp")
  val filePath = new Path(driverFilePath)

  val maxCores = conf.get("spark.simr.executor.cores", "1").toInt

  override def start() {
    super.start()

    val driverUrl = "akka.tcp://spark@%s:%s/user/%s".format(
      sc.conf.get("spark.driver.host"), sc.conf.get("spark.driver.port"),
      CoarseGrainedSchedulerBackend.ACTOR_NAME)

    val conf = new Configuration()
    val fs = FileSystem.get(conf)

    logInfo("Writing to HDFS file: "  + driverFilePath)
    logInfo("Writing Akka address: "  + driverUrl)
    logInfo("Writing Spark UI Address: " + sc.ui.appUIAddress)

    // Create temporary file to prevent race condition where executors get empty driverUrl file
    val temp = fs.create(tmpPath, true)
    temp.writeUTF(driverUrl)
    temp.writeInt(maxCores)
    temp.writeUTF(sc.ui.appUIAddress)
    temp.close()

    // "Atomic" rename
    fs.rename(tmpPath, filePath)
  }

  override def stop() {
    val conf = new Configuration()
    val fs = FileSystem.get(conf)
    fs.delete(new Path(driverFilePath), false)
    super.stop()
  }
}
