package org.apache.spark.graph.impl

import org.apache.spark.graph._
import org.apache.spark.util.collection.PrimitiveKeyOpenHashMap

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
class EdgePartition[@specialized(Char, Int, Boolean, Byte, Long, Float, Double) ED: ClassManifest](
    val srcIds: Array[Vid],
    val dstIds: Array[Vid],
    val data: Array[ED],
    val index: PrimitiveKeyOpenHashMap[Vid, Int]) {

  /**
   * Reverse all the edges in this partition.
   *
   * @return a new edge partition with all edges reversed.
   */
  def reverse: EdgePartition[ED] = {
    val builder = new EdgePartitionBuilder(size)
    for (e <- iterator) {
      builder.add(e.dstId, e.srcId, e.attr)
    }
    builder.toEdgePartition
  }

  /**
   * Construct a new edge partition by applying the function f to all
   * edges in this partition.
   *
   * @param f a function from an edge to a new attribute
   * @tparam ED2 the type of the new attribute
   * @return a new edge partition with the result of the function `f`
   *         applied to each edge
   */
  def map[ED2: ClassManifest](f: Edge[ED] => ED2): EdgePartition[ED2] = {
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
    new EdgePartition(srcIds, dstIds, newData, index)
  }

  /**
   * Construct a new edge partition by using the edge attributes
   * contained in the iterator.
   *
   * @note The input iterator should return edge attributes in the
   * order of the edges returned by `EdgePartition.iterator` and
   * should return attributes equal to the number of edges.
   *
   * @param f a function from an edge to a new attribute
   * @tparam ED2 the type of the new attribute
   * @return a new edge partition with the result of the function `f`
   *         applied to each edge
   */
  def map[ED2: ClassManifest](iter: Iterator[ED2]): EdgePartition[ED2] = {
    val newData = new Array[ED2](data.size)
    var i = 0
    while (iter.hasNext) {
      newData(i) = iter.next()
      i += 1
    }
    assert(newData.size == i)
    new EdgePartition(srcIds, dstIds, newData, index)
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
  def groupEdges(merge: (ED, ED) => ED): EdgePartition[ED] = {
    val builder = new EdgePartitionBuilder[ED]
    var firstIter: Boolean = true
    var currSrcId: Vid = nullValue[Vid]
    var currDstId: Vid = nullValue[Vid]
    var currAttr: ED = nullValue[ED]
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
    builder.toEdgePartition
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
  def innerJoin[ED2: ClassManifest, ED3: ClassManifest]
      (other: EdgePartition[ED2])
      (f: (Vid, Vid, ED, ED2) => ED3): EdgePartition[ED3] = {
    val builder = new EdgePartitionBuilder[ED3]
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
    builder.toEdgePartition
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

  /**
   * Get an iterator over the edges in this partition whose source vertex ids match srcIdPred. The
   * iterator is generated using an index scan, so it is efficient at skipping edges that don't
   * match srcIdPred.
   */
  def indexIterator(srcIdPred: Vid => Boolean): Iterator[Edge[ED]] =
    index.iterator.filter(kv => srcIdPred(kv._1)).flatMap(Function.tupled(clusterIterator))

  /**
   * Get an iterator over the cluster of edges in this partition with source vertex id `srcId`. The
   * cluster must start at position `index`.
   */
  private def clusterIterator(srcId: Vid, index: Int) = new Iterator[Edge[ED]] {
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
}
