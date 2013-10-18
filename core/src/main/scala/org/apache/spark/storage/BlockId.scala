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

package org.apache.spark.storage

/**
 * Identifies a particular Block of data, usually associated with a single file.
 * A Block can be uniquely identified by its filename, but each type of Block has a different
 * set of keys which produce its unique name.
 *
 * If your BlockId should be serializable, be sure to add it to the BlockId.fromString() method.
 */
private[spark] sealed abstract class BlockId {
  /** A globally unique identifier for this Block. Can be used for ser/de. */
  def name: String

  // convenience methods
  def asRDDId = if (isRDD) Some(asInstanceOf[RDDBlockId]) else None
  def isRDD = isInstanceOf[RDDBlockId]
  def isShuffle = isInstanceOf[ShuffleBlockId]
  def isBroadcast = isInstanceOf[BroadcastBlockId]

  override def toString = name
  override def hashCode = name.hashCode
  override def equals(other: Any): Boolean = other match {
    case o: BlockId => getClass == o.getClass && name.equals(o.name)
    case _ => false
  }
}

private[spark] case class RDDBlockId(rddId: Int, splitIndex: Int) extends BlockId {
  def name = "rdd_" + rddId + "_" + splitIndex
}

private[spark]
case class ShuffleBlockId(shuffleId: Int, mapId: Int, reduceId: Int) extends BlockId {
  def name = "shuffle_" + shuffleId + "_" + mapId + "_" + reduceId
}

private[spark] case class BroadcastBlockId(broadcastId: Long) extends BlockId {
  def name = "broadcast_" + broadcastId
}

private[spark] case class TaskResultBlockId(taskId: Long) extends BlockId {
  def name = "taskresult_" + taskId
}

private[spark] case class StreamBlockId(streamId: Int, uniqueId: Long) extends BlockId {
  def name = "input-" + streamId + "-" + uniqueId
}

// Intended only for testing purposes
private[spark] case class TestBlockId(id: String) extends BlockId {
  def name = "test_" + id
}

private[spark] object BlockId {
  val RDD = "rdd_([0-9]+)_([0-9]+)".r
  val SHUFFLE = "shuffle_([0-9]+)_([0-9]+)_([0-9]+)".r
  val BROADCAST = "broadcast_([0-9]+)".r
  val TASKRESULT = "taskresult_([0-9]+)".r
  val STREAM = "input-([0-9]+)-([0-9]+)".r
  val TEST = "test_(.*)".r

  /** Converts a BlockId "name" String back into a BlockId. */
  def apply(id: String) = id match {
    case RDD(rddId, splitIndex) =>
      RDDBlockId(rddId.toInt, splitIndex.toInt)
    case SHUFFLE(shuffleId, mapId, reduceId) =>
      ShuffleBlockId(shuffleId.toInt, mapId.toInt, reduceId.toInt)
    case BROADCAST(broadcastId) =>
      BroadcastBlockId(broadcastId.toLong)
    case TASKRESULT(taskId) =>
      TaskResultBlockId(taskId.toLong)
    case STREAM(streamId, uniqueId) =>
      StreamBlockId(streamId.toInt, uniqueId.toLong)
    case TEST(value) =>
      TestBlockId(value)
    case _ =>
      throw new IllegalStateException("Unrecognized BlockId: " + id)
  }
}
