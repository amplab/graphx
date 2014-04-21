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

private[graphx]
object ShippableVertexPartition {
  def apply[VD: ClassTag](iter: Iterator[(VertexId, VD)]): ShippableVertexPartition[VD] =
    apply(iter, RoutingTablePartition.empty, null.asInstanceOf[VD])

  def apply[VD: ClassTag](
      iter: Iterator[(VertexId, VD)], routingTable: RoutingTablePartition, defaultVal: VD)
    : ShippableVertexPartition[VD] = {
    val fullIter = iter ++ routingTable.iterator.map(vid => (vid, defaultVal))
    val (index, values, mask) = VertexPartitionBase.initFrom(fullIter, (a: VD, b: VD) => a)
    new ShippableVertexPartition(index, values, mask, routingTable)
  }

  // For using ops directly on particular subclasses of VertexPartitionBase
  implicit def shippablePartitionToOps[VD: ClassTag](partition: ShippableVertexPartition[VD]) =
    new ShippableVertexPartitionOps(partition)

  // For using ops on generic VertexPartitionBase types via a context bound
  implicit object ShippableVertexPartitionOpsConstructor extends VertexPartitionBaseOpsConstructor[ShippableVertexPartition] {
    def toOps[VD: ClassTag](partition: ShippableVertexPartition[VD]): VertexPartitionBaseOps[VD, ShippableVertexPartition] =
      shippablePartitionToOps(partition)
  }
}

private[graphx]
class ShippableVertexPartition[VD: ClassTag](
    val index: VertexIdToIndexMap,
    val values: Array[VD],
    val mask: BitSet,
    /**
     * Stores the locations of edge-partition join sites for each vertex attribute; that is, the
     * routing information for shipping vertex attributes to edge partitions.
     */
    val routingTable: RoutingTablePartition)
  extends VertexPartitionBase[VD] {

  // Ship vertex attributes to edge partitions according to vertexPlacement
  def shipVertexAttributes(
      shipSrc: Boolean, shipDst: Boolean): Iterator[(PartitionID, VertexAttributeBlock[VD])] = {
    Iterator.tabulate(routingTable.numEdgePartitions) { pid =>
      val initialSize = if (shipSrc && shipDst) routingTable.partitionSize(pid) else 64
      val vids = new PrimitiveVector[VertexId](initialSize)
      val attrs = new PrimitiveVector[VD](initialSize)
      var i = 0
      routingTable.foreachWithinEdgePartition(pid, shipSrc, shipDst) { vid =>
        if (isDefined(vid)) {
          vids += vid
          attrs += this(vid)
        }
        i += 1
      }
      (pid, new VertexAttributeBlock(vids.trim().array, attrs.trim().array))
    }
  }

  // Ship active sets to edge partitions using vertexPlacement, but ignoring includeSrc and
  // includeDst. These flags govern attribute shipping, but the activeness of a vertex must be
  // shipped to all edges mentioning that vertex, regardless of whether the vertex attribute is
  // also shipped there.
  def shipVertexIds(): Iterator[(PartitionID, Array[VertexId])] = {
    Iterator.tabulate(routingTable.numEdgePartitions) { pid =>
      val vids = new PrimitiveVector[VertexId](routingTable.partitionSize(pid))
      var i = 0
      routingTable.foreachWithinEdgePartition(pid, true, true) { vid =>
        if (isDefined(vid)) {
          vids += vid
        }
        i += 1
      }
      (pid, vids.trim().array)
    }
  }
}

private[graphx] class ShippableVertexPartitionOps[VD: ClassTag](self: ShippableVertexPartition[VD])
  extends VertexPartitionBaseOps[VD, ShippableVertexPartition](self) {

  def withIndex(index: VertexIdToIndexMap): ShippableVertexPartition[VD] = {
    new ShippableVertexPartition(index, self.values, self.mask, self.routingTable)
  }

  def withValues[VD2: ClassTag](values: Array[VD2]): ShippableVertexPartition[VD2] = {
    new ShippableVertexPartition(self.index, values, self.mask, self.routingTable)
  }

  def withMask(mask: BitSet): ShippableVertexPartition[VD] = {
    new ShippableVertexPartition(self.index, self.values, mask, self.routingTable)
  }
}
