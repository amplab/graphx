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

private[graphx] object VertexPartition {
  def apply[VD: ClassTag](iter: Iterator[(VertexId, VD)])
    : VertexPartition[VD] = {
    val (index, values, mask) = VertexPartitionBase.initFrom(iter)
    new VertexPartition(index, values, mask)
  }

  // For using ops directly on particular subclasses of VertexPartitionBase
  implicit def partitionToOps[VD: ClassTag](partition: VertexPartition[VD]) =
    new VertexPartitionOps(partition)

  // For using ops on generic VertexPartitionBase types via a context bound
  implicit object VertexPartitionOpsConstructor extends VertexPartitionBaseOpsConstructor[VertexPartition] {
    def toOps[VD: ClassTag](partition: VertexPartition[VD]): VertexPartitionBaseOps[VD, VertexPartition] =
      partitionToOps(partition)
  }
}
private[graphx] class VertexPartition[VD: ClassTag](
    val index: VertexIdToIndexMap,
    val values: Array[VD],
    val mask: BitSet)
  extends VertexPartitionBase[VD]

private[graphx] class VertexPartitionOps[VD: ClassTag](self: VertexPartition[VD])
  extends VertexPartitionBaseOps[VD, VertexPartition](self) {

  def withIndex(index: VertexIdToIndexMap): VertexPartition[VD] = {
    new VertexPartition(index, self.values, self.mask)
  }

  def withValues[VD2: ClassTag](values: Array[VD2]): VertexPartition[VD2] = {
    new VertexPartition(self.index, values, self.mask)
  }

  def withMask(mask: BitSet): VertexPartition[VD] = {
    new VertexPartition(self.index, self.values, mask)
  }
}
