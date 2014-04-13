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

private[graphx] object VertexPartitionBase {
  // Same effect as initFrom(iter, (a, b) => b)
  def initFrom[VD: ClassTag](iter: Iterator[(VertexId, VD)])
    : (VertexIdToIndexMap, Array[VD], BitSet) = {
    val map = new PrimitiveKeyOpenHashMap[VertexId, VD]
    iter.foreach { case (k, v) =>
      map(k) = v
    }
    (map.keySet, map._values, map.keySet.getBitSet)
  }

  def initFrom[VD: ClassTag](iter: Iterator[(VertexId, VD)], mergeFunc: (VD, VD) => VD)
    : (VertexIdToIndexMap, Array[VD], BitSet) = {
    val map = new PrimitiveKeyOpenHashMap[VertexId, VD]
    iter.foreach { case (k, v) =>
      map.setMerge(k, v, mergeFunc)
    }
    (map.keySet, map._values, map.keySet.getBitSet)
  }
}

private[graphx]
abstract class VertexPartitionBase[
  @specialized(Long, Int, Double) VD: ClassTag] {

  def index: VertexIdToIndexMap
  def values: Array[VD]
  def mask: BitSet

  val capacity: Int = index.capacity

  def size: Int = mask.cardinality()

  /** Return the vertex attribute for the given vertex ID. */
  def apply(vid: VertexId): VD = values(index.getPos(vid))

  def isDefined(vid: VertexId): Boolean = {
    val pos = index.getPos(vid)
    pos >= 0 && mask.get(pos)
  }

  def iterator: Iterator[(VertexId, VD)] =
    mask.iterator.map(ind => (index.getValue(ind), values(ind)))
}

trait VertexPartitionBaseOpsConstructor[T[X] <: VertexPartitionBase[X]] {
  def toOps[VD: ClassTag](partition: T[VD]): VertexPartitionBaseOps[VD, T]
}
