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

package org.apache.spark.deploy

import java.security.PrivilegedExceptionAction

import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.mapred.JobConf
import org.apache.hadoop.security.UserGroupInformation

import org.apache.spark.{SparkContext, SparkException}

/**
 * Contains util methods to interact with Hadoop from Spark.
 */
private[spark]
class SparkHadoopUtil {
  val conf = newConfiguration()
  UserGroupInformation.setConfiguration(conf)

  def runAsUser(user: String)(func: () => Unit) {
    // if we are already running as the user intended there is no reason to do the doAs. It
    // will actually break secure HDFS access as it doesn't fill in the credentials. Also if
    // the user is UNKNOWN then we shouldn't be creating a remote unknown user
    // (this is actually the path spark on yarn takes) since SPARK_USER is initialized only
    // in SparkContext.
    val currentUser = Option(System.getProperty("user.name")).
      getOrElse(SparkContext.SPARK_UNKNOWN_USER)
    if (user != SparkContext.SPARK_UNKNOWN_USER && currentUser != user) {
      val ugi = UserGroupInformation.createRemoteUser(user)
      ugi.doAs(new PrivilegedExceptionAction[Unit] {
        def run: Unit = func()
      })
    } else {
      func()
    }
  }

  /**
   * Return an appropriate (subclass) of Configuration. Creating config can initializes some Hadoop
   * subsystems.
   */
  def newConfiguration(): Configuration = new Configuration()

  /**
   * Add any user credentials to the job conf which are necessary for running on a secure Hadoop
   * cluster.
   */
  def addCredentials(conf: JobConf) {}

  def isYarnMode(): Boolean = { false }
}

object SparkHadoopUtil {

  private val hadoop = {
    val yarnMode = java.lang.Boolean.valueOf(
        System.getProperty("SPARK_YARN_MODE", System.getenv("SPARK_YARN_MODE")))
    if (yarnMode) {
      try {
        Class.forName("org.apache.spark.deploy.yarn.YarnSparkHadoopUtil")
          .newInstance()
          .asInstanceOf[SparkHadoopUtil]
      } catch {
       case th: Throwable => throw new SparkException("Unable to load YARN support", th)
      }
    } else {
      new SparkHadoopUtil
    }
  }

  def get: SparkHadoopUtil = {
    hadoop
  }
}
