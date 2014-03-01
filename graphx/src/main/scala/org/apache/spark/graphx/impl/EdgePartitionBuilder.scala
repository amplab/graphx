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

import scala.reflect.{ClassTag, classTag}
import scala.util.Sorting

import org.apache.spark.graphx._
import org.apache.spark.graphx.util.collection.PrimitiveKeyOpenHashMap
import org.apache.spark.util.collection.PrimitiveVector

object EdgePartitionBuilder {
  private[graphx] def lexicographicOrdering[ED] = new Ordering[Edge[ED]] {
    override def compare(a: Edge[ED], b: Edge[ED]): Int = {
      if (a.srcLocalVid != b.srcLocalVid) {
        a.srcLocalVid - b.srcLocalVid
      } else {
        a.dstLocalVid - b.dstLocalVid
      }
    }
  }
}

private[graphx]
class EdgePartitionBuilder[@specialized(Long, Int, Double) ED: ClassTag](
    size: Int = 64, var vertexIndex: Option[VertexIdToIndexMap] = None) {

  var _vertexIndex: VertexIdToIndexMap = null
  var edges: PrimitiveVector[Edge[ED]] = null
  init(null.asInstanceOf[ED])

  /**
   * Works around a Scala compiler bug:
   * http://axel22.github.io/2013/11/03/specialization-quirks.html#initialize_specialized_values_outside_constructor_body
   */
  private def init(dummyEdge: ED) {
    edges = new PrimitiveVector[Edge[ED]](size)
    _vertexIndex = vertexIndex.getOrElse(new VertexIdToIndexMap)
  }

  /** Add a new edge to the partition. */
  def add(src: VertexId, dst: VertexId, d: ED) {
    _vertexIndex.add(src)
    _vertexIndex.add(dst)
    edges += Edge(src, dst, d)
  }

  /** Add a new edge to the partition using its local vids. */
  def addLocal(srcLocalVid: Int, dstLocalVid: Int, d: ED) {
    val e = Edge(attr = d)
    e.srcLocalVid = srcLocalVid
    e.dstLocalVid = dstLocalVid
    edges += e
  }

  def toEdgePartition: EdgePartition[ED] = {
    if (vertexIndex.nonEmpty) {
      toEdgePartitionHelper(edges.trim().array, _vertexIndex)
    } else {
      val localEdges = edges.trim().array
      var i = 0
      while (i < localEdges.length) {
        localEdges(i).srcLocalVid = _vertexIndex.getPos(localEdges(i).srcId)
        localEdges(i).dstLocalVid = _vertexIndex.getPos(localEdges(i).dstId)
        i += 1
      }
      toEdgePartitionHelper(localEdges, _vertexIndex)
    }
  }

  protected def toEdgePartitionHelper(
      edgeArray: Array[Edge[ED]], vertexIndex: VertexIdToIndexMap): EdgePartition[ED] = {
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
