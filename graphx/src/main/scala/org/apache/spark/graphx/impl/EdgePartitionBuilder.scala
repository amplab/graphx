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
import scala.util.Sorting

import org.apache.spark.graphx._
import org.apache.spark.graphx.util.collection.PrimitiveKeyOpenHashMap
import org.apache.spark.util.collection.PrimitiveVector

private[graphx]
case class LocalEdge[ED](val srcLocalVid: Int, val dstLocalVid: Int, val attr: ED)

object EdgePartitionBuilder {
  private[graphx] def lexicographicOrdering[ED] = new Ordering[LocalEdge[ED]] {
    override def compare(a: LocalEdge[ED], b: LocalEdge[ED]): Int = {
      if (a.srcLocalVid != b.srcLocalVid) {
        a.srcLocalVid - b.srcLocalVid
      } else {
        a.dstLocalVid - b.dstLocalVid
      }
    }
  }
}

private[graphx]
abstract class EdgePartitionBuilder[/*@specialized(Long, Int, Double)*/ ED: ClassTag] {
  def toEdgePartition: EdgePartition[ED]

  protected def toEdgePartitionHelper(
      edgeArray: Array[LocalEdge[ED]], vertexIndex: VertexIdToIndexMap): EdgePartition[ED] = {
    Sorting.quickSort(edgeArray)(EdgePartitionBuilder.lexicographicOrdering[ED])
    val srcIds = new Array[Int](edgeArray.size)
    val dstIds = new Array[Int](edgeArray.size)
    val data = new Array[ED](edgeArray.size)
    val index = new PrimitiveKeyOpenHashMap[Int, Int]
    // Copy edges into columnar structures, tracking the beginnings of source vertex id clusters and
    // adding them to the index
    if (edgeArray.length > 0) {
      index.update(srcIds(0), 0)
      var currSrcId = srcIds(0)
      var i = 0
      while (i < edgeArray.size) {
        srcIds(i) = edgeArray(i).srcLocalVid
        dstIds(i) = edgeArray(i).dstLocalVid
        data(i) = edgeArray(i).attr
        if (edgeArray(i).srcLocalVid != currSrcId) {
          currSrcId = edgeArray(i).srcLocalVid
          index.update(currSrcId, i)
        }
        i += 1
      }
    }
    new EdgePartition(srcIds, dstIds, data, index, vertexIndex)
  }
}

/**
 * Creates an EdgePartition from an existing vertexIndex.
 */
private[graphx]
class ExistingEdgePartitionBuilder[/*@specialized(Long, Int, Double)*/ ED: ClassTag](
    vertexIndex: VertexIdToIndexMap, size: Int = 64) extends EdgePartitionBuilder[ED] {

  var edges = new PrimitiveVector[LocalEdge[ED]](size)

  /** Add a new edge to the partition using its local vids. */
  def add(srcLocalVid: Int, dstLocalVid: Int, d: ED) {
    edges += LocalEdge(srcLocalVid, dstLocalVid, d)
  }

  def toEdgePartition: EdgePartition[ED] = {
    toEdgePartitionHelper(edges.trim().array, vertexIndex)
  }
}

/**
 * Creates an EdgePartition from scratch.
 */
private[graphx]
class FreshEdgePartitionBuilder[/*@specialized(Long, Int, Double)*/ ED: ClassTag](
    size: Int = 64) extends EdgePartitionBuilder[ED] {

  private val vertexIndex: VertexIdToIndexMap = new VertexIdToIndexMap

  var edges = new PrimitiveVector[Edge[ED]](size)

  /** Add a new edge to the partition. */
  def add(src: VertexId, dst: VertexId, d: ED) {
    vertexIndex.add(src)
    vertexIndex.add(dst)
    edges += Edge(src, dst, d)
  }

  def toEdgePartition: EdgePartition[ED] = {
    val localEdges = edges.trim().array.map(e =>
      LocalEdge(vertexIndex.getPos(e.srcId), vertexIndex.getPos(e.dstId), e.attr))
    toEdgePartitionHelper(localEdges, vertexIndex)
  }
}
