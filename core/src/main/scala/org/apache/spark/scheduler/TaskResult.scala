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

package org.apache.spark.scheduler

import java.io._

import scala.collection.mutable.Map
import org.apache.spark.executor.TaskMetrics
import org.apache.spark.{SparkEnv}
import java.nio.ByteBuffer
import org.apache.spark.util.Utils
import org.apache.spark.storage.BlockId

// Task result. Also contains updates to accumulator variables.
private[spark] sealed trait TaskResult[T]

/** A reference to a DirectTaskResult that has been stored in the worker's BlockManager. */
private[spark]
case class IndirectTaskResult[T](blockId: BlockId) extends TaskResult[T] with Serializable

/** A TaskResult that contains the task's return value and accumulator updates. */
private[spark]
class DirectTaskResult[T](var value: T, var accumUpdates: Map[Long, Any], var metrics: TaskMetrics)
  extends TaskResult[T] with Externalizable {

  def this() = this(null.asInstanceOf[T], null, null)

  override def writeExternal(out: ObjectOutput) {

    val objectSer = SparkEnv.get.serializer.newInstance()
    val bb = objectSer.serialize(value)

    out.writeInt(bb.remaining())
    Utils.writeByteBuffer(bb, out)

    out.writeInt(accumUpdates.size)
    for ((key, value) <- accumUpdates) {
      out.writeLong(key)
      out.writeObject(value)
    }
    out.writeObject(metrics)
  }

  override def readExternal(in: ObjectInput) {

    val objectSer = SparkEnv.get.serializer.newInstance()

    val blen = in.readInt()
    val byteVal = new Array[Byte](blen)
    in.readFully(byteVal)
    value = objectSer.deserialize(ByteBuffer.wrap(byteVal))

    val numUpdates = in.readInt
    if (numUpdates == 0) {
      accumUpdates = null
    } else {
      accumUpdates = Map()
      for (i <- 0 until numUpdates) {
        accumUpdates(in.readLong()) = in.readObject()
      }
    }
    metrics = in.readObject().asInstanceOf[TaskMetrics]
  }
}
