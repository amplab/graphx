package org.apache.spark.graph.impl

import org.apache.spark.util.collection.{BitSet, PrimitiveKeyOpenHashMap}

import org.apache.spark.Logging
import org.apache.spark.graph._


private[graph] object VertexPartition {

  def apply[VD: ClassManifest](iter: Iterator[(Vid, VD)]): VertexPartition[VD] = {
    val map = new PrimitiveKeyOpenHashMap[Vid, VD]
    iter.foreach { case (k, v) =>
      map(k) = v
    }
    new VertexPartition(map.keySet, map._values, map.keySet.getBitSet)
  }

  def apply[VD: ClassManifest](iter: Iterator[(Vid, VD)], mergeFunc: (VD, VD) => VD)
    : VertexPartition[VD] =
  {
    val map = new PrimitiveKeyOpenHashMap[Vid, VD]
    iter.foreach { case (k, v) =>
      map.setMerge(k, v, mergeFunc)
    }
    new VertexPartition(map.keySet, map._values, map.keySet.getBitSet)
  }
}


private[graph]
class VertexPartition[@specialized(Long, Int, Double) VD: ClassManifest](
    val index: VertexIdToIndexMap,
    val values: Array[VD],
    val mask: BitSet,
    /** A set of vids of active vertices. May contain vids not in index due to join rewrite. */
    private val activeSet: Option[VertexSet] = None)
  extends Logging {

  val capacity: Int = index.capacity

  def size: Int = mask.cardinality()

  /** Return the vertex attribute for the given vertex ID. */
  def apply(vid: Vid): VD = values(index.getPos(vid))

  def isDefined(vid: Vid): Boolean = {
    val pos = index.getPos(vid)
    pos >= 0 && mask.get(pos)
  }

  /** Look up vid in activeSet, throwing an exception if it is None. */
  def isActive(vid: Vid): Boolean = {
    activeSet.get.contains(vid)
  }

  /** The number of active vertices, if any exist. */
  def numActives: Option[Int] = activeSet.map(_.size)

  /**
   * Pass each vertex attribute along with the vertex id through a map
   * function and retain the original RDD's partitioning and index.
   *
   * @tparam VD2 the type returned by the map function
   *
   * @param f the function applied to each vertex id and vertex
   * attribute in the RDD
   *
   * @return a new VertexPartition with values obtained by applying `f` to
   * each of the entries in the original VertexRDD.  The resulting
   * VertexPartition retains the same index.
   */
  def map[VD2: ClassManifest](f: (Vid, VD) => VD2): VertexPartition[VD2] = {
    // Construct a view of the map transformation
    val newValues = new Array[VD2](capacity)
    var i = mask.nextSetBit(0)
    while (i >= 0) {
      newValues(i) = f(index.getValue(i), values(i))
      i = mask.nextSetBit(i + 1)
    }
    new VertexPartition[VD2](index, newValues, mask)
  }

  /**
   * Restrict the vertex set to the set of vertices satisfying the given predicate.
   *
   * @param pred the user defined predicate
   *
   * @note The vertex set preserves the original index structure which means that the returned
   *       RDD can be easily joined with the original vertex-set. Furthermore, the filter only
   *       modifies the bitmap index and so no new values are allocated.
   */
  def filter(pred: (Vid, VD) => Boolean): VertexPartition[VD] = {
    // Allocate the array to store the results into
    val newMask = new BitSet(capacity)
    // Iterate over the active bits in the old mask and evaluate the predicate
    var i = mask.nextSetBit(0)
    while (i >= 0) {
      if (pred(index.getValue(i), values(i))) {
        newMask.set(i)
      }
      i = mask.nextSetBit(i + 1)
    }
    new VertexPartition(index, values, newMask)
  }

  /**
   * Hides vertices that are the same between this and other. For vertices that are different, keeps
   * the values from `other`. The indices of `this` and `other` must be the same.
   */
  def diff(other: VertexPartition[VD]): VertexPartition[VD] = {
    if (index != other.index) {
      logWarning("Diffing two VertexPartitions with different indexes is slow.")
      diff(createUsingIndex(other.iterator))
    } else {
      val newMask = mask & other.mask
      var i = newMask.nextSetBit(0)
      while (i >= 0) {
        if (values(i) == other.values(i)) {
          newMask.unset(i)
        }
        i = newMask.nextSetBit(i + 1)
      }
      new VertexPartition(index, other.values, newMask)
    }
  }

  /** Left outer join another VertexPartition. */
  def leftJoin[VD2: ClassManifest, VD3: ClassManifest]
      (other: VertexPartition[VD2])
      (f: (Vid, VD, Option[VD2]) => VD3): VertexPartition[VD3] = {
    if (index != other.index) {
      logWarning("Joining two VertexPartitions with different indexes is slow.")
      leftJoin(createUsingIndex(other.iterator))(f)
    } else {
      val newValues = new Array[VD3](capacity)

      var i = mask.nextSetBit(0)
      while (i >= 0) {
        val otherV: Option[VD2] = if (other.mask.get(i)) Some(other.values(i)) else None
        newValues(i) = f(index.getValue(i), values(i), otherV)
        i = mask.nextSetBit(i + 1)
      }
      new VertexPartition(index, newValues, mask)
    }
  }

  /** Left outer join another iterator of messages. */
  def leftJoin[VD2: ClassManifest, VD3: ClassManifest]
      (other: Iterator[(Vid, VD2)])
      (f: (Vid, VD, Option[VD2]) => VD3): VertexPartition[VD3] = {
    leftJoin(createUsingIndex(other))(f)
  }

  /** Inner join another VertexPartition. */
  def innerJoin[U: ClassManifest, VD2: ClassManifest](other: VertexPartition[U])
      (f: (Vid, VD, U) => VD2): VertexPartition[VD2] = {
    if (index != other.index) {
      logWarning("Joining two VertexPartitions with different indexes is slow.")
      innerJoin(createUsingIndex(other.iterator))(f)
    } else {
      val newMask = mask & other.mask
      val newValues = new Array[VD2](capacity)
      var i = newMask.nextSetBit(0)
      while (i >= 0) {
        newValues(i) = f(index.getValue(i), values(i), other.values(i))
        i = newMask.nextSetBit(i + 1)
      }
      new VertexPartition(index, newValues, newMask)
    }
  }

  /**
   * Inner join an iterator of messages.
   */
  def innerJoin[U: ClassManifest, VD2: ClassManifest]
      (iter: Iterator[Product2[Vid, U]])
      (f: (Vid, VD, U) => VD2): VertexPartition[VD2] = {
    innerJoin(createUsingIndex(iter))(f)
  }

  /**
   * Similar effect as aggregateUsingIndex((a, b) => a)
   */
  def createUsingIndex[VD2: ClassManifest](iter: Iterator[Product2[Vid, VD2]])
    : VertexPartition[VD2] = {
    val newMask = new BitSet(capacity)
    val newValues = new Array[VD2](capacity)
    iter.foreach { case (vid, vdata) =>
      val pos = index.getPos(vid)
      if (pos >= 0) {
        newMask.set(pos)
        newValues(pos) = vdata
      }
    }
    new VertexPartition[VD2](index, newValues, newMask)
  }

  /**
   * Similar to innerJoin, but vertices from the left side that don't appear in iter will remain in
   * the partition, hidden by the bitmask.
   */
  def innerJoinKeepLeft(iter: Iterator[Product2[Vid, VD]]): VertexPartition[VD] = {
    val newMask = new BitSet(capacity)
    val newValues = new Array[VD](capacity)
    System.arraycopy(values, 0, newValues, 0, newValues.length)
    iter.foreach { case (vid, vdata) =>
      val pos = index.getPos(vid)
      if (pos >= 0) {
        newMask.set(pos)
        newValues(pos) = vdata
      }
    }
    new VertexPartition(index, newValues, newMask)
  }

  def aggregateUsingIndex[VD2: ClassManifest](
      iter: Iterator[Product2[Vid, VD2]], reduceFunc: (VD2, VD2) => VD2): VertexPartition[VD2] =
  {
    val newMask = new BitSet(capacity)
    val newValues = new Array[VD2](capacity)
    iter.foreach { product =>
      val vid = product._1
      val vdata = product._2
      val pos = index.getPos(vid)
      if (pos >= 0) {
        if (newMask.get(pos)) {
          newValues(pos) = reduceFunc(newValues(pos), vdata)
        } else { // otherwise just store the new value
          newMask.set(pos)
          newValues(pos) = vdata
        }
      }
    }
    new VertexPartition[VD2](index, newValues, newMask)
  }

  def replaceActives(iter: Iterator[Vid]): VertexPartition[VD] = {
    val newActiveSet = new VertexSet
    iter.foreach(newActiveSet.add(_))
    new VertexPartition(index, values, mask, Some(newActiveSet))
  }

  /**
   * Construct a new VertexPartition whose index contains only the vertices in the mask.
   */
  def reindex(): VertexPartition[VD] = {
    val hashMap = new PrimitiveKeyOpenHashMap[Vid, VD]
    val arbitraryMerge = (a: VD, b: VD) => a
    for ((k, v) <- this.iterator) {
      hashMap.setMerge(k, v, arbitraryMerge)
    }
    new VertexPartition(hashMap.keySet, hashMap._values, hashMap.keySet.getBitSet)
  }

  def iterator: Iterator[(Vid, VD)] = mask.iterator.map(ind => (index.getValue(ind), values(ind)))

  def vidIterator: Iterator[Vid] = mask.iterator.map(ind => index.getValue(ind))
}
