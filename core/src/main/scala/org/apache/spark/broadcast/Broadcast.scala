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

package org.apache.spark.broadcast

import java.io._
import java.util.concurrent.atomic.AtomicLong

import org.apache.spark._

abstract class Broadcast[T](private[spark] val id: Long) extends Serializable {
  def value: T

  // We cannot have an abstract readObject here due to some weird issues with
  // readObject having to be 'private' in sub-classes.

  override def toString = "Broadcast(" + id + ")"
}

private[spark]
class BroadcastManager(val _isDriver: Boolean, conf: SparkConf) extends Logging with Serializable {

  private var initialized = false
  private var broadcastFactory: BroadcastFactory = null

  initialize()

  // Called by SparkContext or Executor before using Broadcast
  private def initialize() {
    synchronized {
      if (!initialized) {
        val broadcastFactoryClass = conf.get(
          "spark.broadcast.factory", "org.apache.spark.broadcast.HttpBroadcastFactory")

        broadcastFactory =
          Class.forName(broadcastFactoryClass).newInstance.asInstanceOf[BroadcastFactory]

        // Initialize appropriate BroadcastFactory and BroadcastObject
        broadcastFactory.initialize(isDriver, conf)

        initialized = true
      }
    }
  }

  def stop() {
    broadcastFactory.stop()
  }

  private val nextBroadcastId = new AtomicLong(0)

  def newBroadcast[T](value_ : T, isLocal: Boolean) =
    broadcastFactory.newBroadcast[T](value_, isLocal, nextBroadcastId.getAndIncrement())

  def isDriver = _isDriver
}
