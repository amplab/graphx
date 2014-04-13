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

package org.apache.spark.graphx.impl

import scala.reflect.ClassTag

import org.apache.spark.Logging
import org.apache.spark.graphx._
import org.apache.spark.graphx.util.collection.PrimitiveKeyOpenHashMap
import org.apache.spark.util.collection.BitSet
import org.apache.spark.util.collection.PrimitiveVector

private[graphx] object RoutingTablePartition {

  val empty: RoutingTablePartition = new RoutingTablePartition(Array.empty)

  def edgePartitionToMsgs(pid: PartitionID, edgePartition: EdgePartition[_, _])
    : Iterator[(VertexId, (PartitionID, Byte))] = {
    // Determine which positions each vertex id appears in using a map where the low 2 bits
    // represent src and dst
    val map = new PrimitiveKeyOpenHashMap[VertexId, Byte]
    edgePartition.srcIds.iterator.foreach { srcId =>
      map.changeValue(srcId, 0x1, (b: Byte) => (b | 0x1).toByte)
    }
    edgePartition.dstIds.iterator.foreach { dstId =>
      map.changeValue(dstId, 0x2, (b: Byte) => (b | 0x2).toByte)
    }
    map.iterator.map { case (vid, byte) => (vid, (pid, byte)) }
  }

  def fromMsgs(numEdgePartitions: Int, iter: Iterator[(VertexId, (PartitionID, Byte))])
    : RoutingTablePartition = {
    val pid2vid = Array.fill(numEdgePartitions)(new PrimitiveVector[VertexId])
    val srcFlags = Array.fill(numEdgePartitions)(new PrimitiveVector[Boolean])
    val dstFlags = Array.fill(numEdgePartitions)(new PrimitiveVector[Boolean])
    for ((vid, (pid, byte)) <- iter) {
      pid2vid(pid) += vid
      srcFlags(pid) += (byte & 0x1) != 0
      dstFlags(pid) += (byte & 0x2) != 0
    }

    new RoutingTablePartition(pid2vid.zipWithIndex.map {
      case (vids, pid) => (vids.trim().array, toBitSet(srcFlags(pid)), toBitSet(dstFlags(pid)))
    })
  }

  private def toBitSet(flags: PrimitiveVector[Boolean]): BitSet = {
    val bitset = new BitSet(flags.size)
    var i = 0
    while (i < flags.size) {
      if (flags(i)) {
        bitset.set(i)
      }
      i += 1
    }
    bitset
  }
}

private[graphx] class RoutingTablePartition(
    private val routingTable: Array[(Array[VertexId], BitSet, BitSet)]) {

  val numEdgePartitions: Int = routingTable.size

  def partitionSize(pid: PartitionID): Int = routingTable(pid)._1.size

  def iterator: Iterator[VertexId] = routingTable.iterator.flatMap(_._1.iterator)

  def foreachWithinEdgePartition
      (pid: PartitionID, includeSrc: Boolean, includeDst: Boolean)
      (f: VertexId => Unit) {
    val (vidsCandidate, srcVids, dstVids) = routingTable(pid)
    val size = vidsCandidate.length
    if (includeSrc && includeDst) {
      // Avoid checks for performance
      vidsCandidate.iterator.foreach(f)
    } else if (!includeSrc && !includeDst) {
      // Do nothing
    } else {
      val relevantVids = if (includeSrc) srcVids else dstVids
      relevantVids.iterator.foreach { i => f(vidsCandidate(i)) }
    }
  }
}
