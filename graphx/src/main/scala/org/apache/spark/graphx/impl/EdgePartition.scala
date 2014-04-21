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

import scala.reflect.{classTag, ClassTag}

import org.apache.spark.graphx._
import org.apache.spark.graphx.util.collection.PrimitiveKeyOpenHashMap

/**
 * A collection of edges stored in 3 large columnar arrays (src, dst, attribute). The arrays are
 * clustered by src.
 *
 * @param srcIds the source vertex id of each edge
 * @param dstIds the destination vertex id of each edge
 * @param data the attribute associated with each edge
 * @param index a clustered index on source vertex id
 * @tparam ED the edge attribute type.
 */
private[graphx]
class EdgePartition[@specialized(Char, Int, Boolean, Byte, Long, Float, Double) ED: ClassTag, VD: ClassTag](
    val srcIds: Array[VertexId],
    val dstIds: Array[VertexId],
    val data: Array[ED],
    val index: PrimitiveKeyOpenHashMap[VertexId, Int],
    // Must include all vids mentioned in srcIds and dstIds. Mask is never used.
    val vertexPartition: VertexPartition[VD],
    /** A set of vids of active vertices. May contain vids not in index due to join rewrite. */
    val activeSet: Option[VertexSet] = None
  ) extends Serializable {

  /** Look up vid in activeSet, throwing an exception if it is None. */
  def isActive(vid: VertexId): Boolean = {
    activeSet.get.contains(vid)
  }

  /** The number of active vertices, if any exist. */
  def numActives: Option[Int] = activeSet.map(_.size)

  def withData[ED2: ClassTag](data_ : Array[ED2]): EdgePartition[ED2, VD] = {
    new EdgePartition(srcIds, dstIds, data_, index, vertexPartition, activeSet)
  }

  def withVertexPartition[VD2: ClassTag](
      vertexPartition_ : VertexPartition[VD2]): EdgePartition[ED, VD2] = {
    new EdgePartition(srcIds, dstIds, data, index, vertexPartition_, activeSet)
  }

  def withActiveSet(iter: Iterator[VertexId]): EdgePartition[ED, VD] = {
    val newActiveSet = new VertexSet
    iter.foreach(newActiveSet.add(_))
    new EdgePartition(srcIds, dstIds, data, index, vertexPartition, Some(newActiveSet))
  }

  def withActiveSet(activeSet_ : Option[VertexSet]): EdgePartition[ED, VD] = {
    new EdgePartition(srcIds, dstIds, data, index, vertexPartition, activeSet_)
  }

  // iter must contain only vertices already in the vertex partition
  def updateVertices(iter: Iterator[(VertexId, VD)]): EdgePartition[ED, VD] = {
    this.withVertexPartition(vertexPartition.innerJoinKeepLeft(iter))
  }

  /**
   * Reverse all the edges in this partition.
   *
   * @return a new edge partition with all edges reversed.
   */
  def reverse: EdgePartition[ED, VD] = {
    val builder = new EdgePartitionBuilder(size)(classTag[ED], classTag[VD])
    for (e <- iterator) {
      builder.add(e.dstId, e.srcId, e.attr)
    }
    builder.toEdgePartition.withVertexPartition(vertexPartition).withActiveSet(activeSet)
  }

  /**
   * Construct a new edge partition by applying the function f to all
   * edges in this partition.
   *
   * Be careful not to keep references to the objects passed to `f`.
   * To improve GC performance the same object is re-used for each call.
   *
   * @param f a function from an edge to a new attribute
   * @tparam ED2 the type of the new attribute
   * @return a new edge partition with the result of the function `f`
   *         applied to each edge
   */
  def map[ED2: ClassTag](f: Edge[ED] => ED2): EdgePartition[ED2, VD] = {
    val newData = new Array[ED2](data.size)
    val edge = new Edge[ED]()
    val size = data.size
    var i = 0
    while (i < size) {
      edge.srcId  = srcIds(i)
      edge.dstId  = dstIds(i)
      edge.attr = data(i)
      newData(i) = f(edge)
      i += 1
    }
    this.withData(newData)
  }

  /**
   * Construct a new edge partition by using the edge attributes
   * contained in the iterator.
   *
   * @note The input iterator should return edge attributes in the
   * order of the edges returned by `EdgePartition.iterator` and
   * should return attributes equal to the number of edges.
   *
   * @param iter an iterator for the new attribute values
   * @tparam ED2 the type of the new attribute
   * @return a new edge partition with the attribute values replaced
   */
  def map[ED2: ClassTag](iter: Iterator[ED2]): EdgePartition[ED2, VD] = {
    // Faster than iter.toArray, because the expected size is known.
    val newData = new Array[ED2](data.size)
    var i = 0
    while (iter.hasNext) {
      newData(i) = iter.next()
      i += 1
    }
    assert(newData.size == i)
    this.withData(newData)
  }

  def filter(
      epred: EdgeTriplet[VD, ED] => Boolean,
      vpred: (VertexId, VD) => Boolean): EdgePartition[ED, VD] = {
    val filtered = tripletIterator().filter(et =>
      vpred(et.srcId, et.srcAttr) && vpred(et.dstId, et.dstAttr) && epred(et))
    val builder = new EdgePartitionBuilder[ED, VD]
    for (e <- filtered) {
      builder.add(e.srcId, e.dstId, e.attr)
    }
    builder.toEdgePartition.withVertexPartition(vertexPartition).withActiveSet(activeSet)
  }

  /**
   * Apply the function f to all edges in this partition.
   *
   * @param f an external state mutating user defined function.
   */
  def foreach(f: Edge[ED] => Unit) {
    iterator.foreach(f)
  }

  /**
   * Merge all the edges with the same src and dest id into a single
   * edge using the `merge` function
   *
   * @param merge a commutative associative merge operation
   * @return a new edge partition without duplicate edges
   */
  def groupEdges(merge: (ED, ED) => ED): EdgePartition[ED, VD] = {
    val builder = new EdgePartitionBuilder[ED, VD]
    var currSrcId: VertexId = null.asInstanceOf[VertexId]
    var currDstId: VertexId = null.asInstanceOf[VertexId]
    var currAttr: ED = null.asInstanceOf[ED]
    var i = 0
    while (i < size) {
      if (i > 0 && currSrcId == srcIds(i) && currDstId == dstIds(i)) {
        currAttr = merge(currAttr, data(i))
      } else {
        if (i > 0) {
          builder.add(currSrcId, currDstId, currAttr)
        }
        currSrcId = srcIds(i)
        currDstId = dstIds(i)
        currAttr = data(i)
      }
      i += 1
    }
    if (size > 0) {
      builder.add(currSrcId, currDstId, currAttr)
    }
    builder.toEdgePartition.withVertexPartition(vertexPartition).withActiveSet(activeSet)
  }

  /**
   * Apply `f` to all edges present in both `this` and `other` and return a new EdgePartition
   * containing the resulting edges.
   *
   * If there are multiple edges with the same src and dst in `this`, `f` will be invoked once for
   * each edge, but each time it may be invoked on any corresponding edge in `other`.
   *
   * If there are multiple edges with the same src and dst in `other`, `f` will only be invoked
   * once.
   */
  def innerJoin[ED2: ClassTag, ED3: ClassTag]
      (other: EdgePartition[ED2, _])
      (f: (VertexId, VertexId, ED, ED2) => ED3): EdgePartition[ED3, VD] = {
    val builder = new EdgePartitionBuilder[ED3, VD]
    var i = 0
    var j = 0
    // For i = index of each edge in `this`...
    while (i < size && j < other.size) {
      val srcId = this.srcIds(i)
      val dstId = this.dstIds(i)
      // ... forward j to the index of the corresponding edge in `other`, and...
      while (j < other.size && other.srcIds(j) < srcId) { j += 1 }
      if (j < other.size && other.srcIds(j) == srcId) {
        while (j < other.size && other.srcIds(j) == srcId && other.dstIds(j) < dstId) { j += 1 }
        if (j < other.size && other.srcIds(j) == srcId && other.dstIds(j) == dstId) {
          // ... run `f` on the matching edge
          builder.add(srcId, dstId, f(srcId, dstId, this.data(i), other.data(j)))
        }
      }
      i += 1
    }
    builder.toEdgePartition.withVertexPartition(vertexPartition).withActiveSet(activeSet)
  }

  /**
   * The number of edges in this partition
   *
   * @return size of the partition
   */
  def size: Int = srcIds.size

  /** The number of unique source vertices in the partition. */
  def indexSize: Int = index.size

  /**
   * Get an iterator over the edges in this partition.
   *
   * Be careful not to keep references to the objects from this iterator.
   * To improve GC performance the same object is re-used in `next()`.
   *
   * @return an iterator over edges in the partition
   */
  def iterator = new Iterator[Edge[ED]] {
    private[this] val edge = new Edge[ED]
    private[this] var pos = 0

    override def hasNext: Boolean = pos < EdgePartition.this.size

    override def next(): Edge[ED] = {
      edge.srcId = srcIds(pos)
      edge.dstId = dstIds(pos)
      edge.attr = data(pos)
      pos += 1
      edge
    }
  }

  // TODO: make a version that reuses objects
  def tripletIterator(
      includeSrc: Boolean = true, includeDst: Boolean = true): Iterator[EdgeTriplet[VD, ED]] = {
    new EdgeTripletIterator(vertexPartition.index, vertexPartition.values, this,
      includeSrc, includeDst)
  }

  def upgradeIterator(
      edgeIter: Iterator[Edge[ED]], includeSrc: Boolean = true, includeDst: Boolean = true)
    : Iterator[EdgeTriplet[VD, ED]] = {
    val tripletIter = new Iterator[EdgeTriplet[VD, ED]] {
      private[this] val triplet = new EdgeTriplet[VD, ED]
      override def hasNext = edgeIter.hasNext
      override def next() = {
        triplet.set(edgeIter.next())
      }
    }
    val withSrc =
      if (includeSrc) {
        tripletIter.map { triplet =>
          triplet.srcAttr = EdgePartition.this.vertexPartition(triplet.srcId)
          triplet
        }
      } else {
        tripletIter
      }
    val withDst =
      if (includeDst) {
        withSrc.map { triplet =>
          triplet.dstAttr = EdgePartition.this.vertexPartition(triplet.dstId)
          triplet
        }
      } else {
        withSrc
      }
    withDst
  }

  /**
   * Get an iterator over the edges in this partition whose source vertex ids match srcIdPred. The
   * iterator is generated using an index scan, so it is efficient at skipping edges that don't
   * match srcIdPred.
   */
  def indexIterator(srcIdPred: VertexId => Boolean): Iterator[Edge[ED]] =
    index.iterator.filter(kv => srcIdPred(kv._1)).flatMap(Function.tupled(clusterIterator))

  /**
   * Get an iterator over the cluster of edges in this partition with source vertex id `srcId`. The
   * cluster must start at position `index`.
   *
   * Be careful not to keep references to the objects from this iterator. To improve GC performance
   * the same object is re-used in `next()`.
   */
  private def clusterIterator(srcId: VertexId, index: Int) = new Iterator[Edge[ED]] {
    private[this] val edge = new Edge[ED]
    private[this] var pos = index

    override def hasNext: Boolean = {
      pos >= 0 && pos < EdgePartition.this.size && srcIds(pos) == srcId
    }

    override def next(): Edge[ED] = {
      assert(srcIds(pos) == srcId)
      edge.srcId = srcIds(pos)
      edge.dstId = dstIds(pos)
      edge.attr = data(pos)
      pos += 1
      edge
    }
  }

  def vidIterator: Iterator[VertexId] = vertexPartition.index.iterator
}
