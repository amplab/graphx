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

package org.apache.spark.graph

import org.apache.spark._
import org.apache.spark.SparkContext._
import org.apache.spark.rdd._
import org.apache.spark.storage.StorageLevel

import org.apache.spark.graph.impl.MsgRDDFunctions
import org.apache.spark.graph.impl.VertexPartition


/**
 * A `VertexRDD[VD]` extends the `RDD[(Vid, VD)]` by ensuring that there is
 * only one entry for each vertex and by pre-indexing the entries for fast,
 * efficient joins.
 *
 * @tparam VD the vertex attribute associated with each vertex in the set.
 *
 * To construct a `VertexRDD` use the singleton object:
 *
 * @example Construct a `VertexRDD` from a plain RDD
 * {{{
 * // Construct an intial vertex set
 * val someData: RDD[(Vid, SomeType)] = loadData(someFile)
 * val vset = VertexRDD(someData)
 * // If there were redundant values in someData we would use a reduceFunc
 * val vset2 = VertexRDD(someData, reduceFunc)
 * // Finally we can use the VertexRDD to index another dataset
 * val otherData: RDD[(Vid, OtherType)] = loadData(otherFile)
 * val vset3 = VertexRDD(otherData, vset.index)
 * // Now we can construct very fast joins between the two sets
 * val vset4: VertexRDD[(SomeType, OtherType)] = vset.leftJoin(vset3)
 * }}}
 *
 */
class VertexRDD[@specialized VD: ClassManifest](
    val partitionsRDD: RDD[VertexPartition[VD]])
  extends RDD[(Vid, VD)](partitionsRDD.context, List(new OneToOneDependency(partitionsRDD))) {

  require(partitionsRDD.partitioner.isDefined)

  /**
   * Construct a new VertexRDD that is indexed by only the keys in the RDD.
   * The resulting VertexSet will be based on a different index and can
   * no longer be quickly joined with this RDD.
   */
  def reindex(): VertexRDD[VD] = new VertexRDD(partitionsRDD.map(_.reindex()))

  /**
   * The partitioner is defined by the index.
   */
  override val partitioner = partitionsRDD.partitioner

  /**
   * The actual partitions are defined by the tuples.
   */
  override protected def getPartitions: Array[Partition] = partitionsRDD.partitions

  /**
   * The preferred locations are computed based on the preferred
   * locations of the tuples.
   */
  override protected def getPreferredLocations(s: Partition): Seq[String] =
    partitionsRDD.preferredLocations(s)

  /**
   * Caching a VertexRDD causes the index and values to be cached separately.
   */
  override def persist(newLevel: StorageLevel): VertexRDD[VD] = {
    partitionsRDD.persist(newLevel)
    this
  }

  /** Persist this RDD with the default storage level (`MEMORY_ONLY`). */
  override def persist(): VertexRDD[VD] = persist(StorageLevel.MEMORY_ONLY)

  /** Persist this RDD with the default storage level (`MEMORY_ONLY`). */
  override def cache(): VertexRDD[VD] = persist()

  /** Return the number of vertices in this set. */
  override def count(): Long = {
    partitionsRDD.map(_.size).reduce(_ + _)
  }

  /**
   * Provide the `RDD[(Vid, VD)]` equivalent output.
   */
  override def compute(part: Partition, context: TaskContext): Iterator[(Vid, VD)] = {
    partitionsRDD.compute(part, context).next().iterator
  }

  /**
   * Return a new VertexRDD by applying a function to each VertexPartition of this RDD.
   */
  def mapVertexPartitions[VD2: ClassManifest](f: VertexPartition[VD] => VertexPartition[VD2])
    : VertexRDD[VD2] = {
    val cleanF = sparkContext.clean(f)
    val newPartitionsRDD = partitionsRDD.mapPartitions(_.map(cleanF), preservesPartitioning = true)
    new VertexRDD(newPartitionsRDD)
  }

  /**
   * Return a new VertexRDD by applying a function to corresponding
   * VertexPartitions of this VertexRDD and another one.
   */
  private def zipVertexPartitions[VD2: ClassManifest, VD3: ClassManifest]
    (other: VertexRDD[VD2])
    (f: (VertexPartition[VD], VertexPartition[VD2]) => VertexPartition[VD3]): VertexRDD[VD3] = {
    val cleanF = sparkContext.clean(f)
    val newPartitionsRDD = partitionsRDD.zipPartitions(
      other.partitionsRDD, preservesPartitioning = true
    ) { (thisIter, otherIter) =>
      val thisPart = thisIter.next()
      val otherPart = otherIter.next()
      Iterator(cleanF(thisPart, otherPart))
    }
    new VertexRDD(newPartitionsRDD)
  }

  /**
   * Restrict the vertex set to the set of vertices satisfying the
   * given predicate.
   *
   * @param pred the user defined predicate, which takes a tuple to conform to
   * the RDD[(Vid, VD)] interface
   *
   * @note The vertex set preserves the original index structure
   * which means that the returned RDD can be easily joined with
   * the original vertex-set.  Furthermore, the filter only
   * modifies the bitmap index and so no new values are allocated.
   */
  override def filter(pred: Tuple2[Vid, VD] => Boolean): VertexRDD[VD] =
    this.mapVertexPartitions(_.filter(Function.untupled(pred)))

  /**
   * Pass each vertex attribute through a map function and retain the
   * original RDD's partitioning and index.
   *
   * @tparam VD2 the type returned by the map function
   *
   * @param f the function applied to each value in the RDD
   * @return a new VertexRDD with values obtained by applying `f` to
   * each of the entries in the original VertexSet.  The resulting
   * VertexRDD retains the same index.
   */
  def mapValues[VD2: ClassManifest](f: VD => VD2): VertexRDD[VD2] =
    this.mapVertexPartitions(_.map((vid, attr) => f(attr)))

  /**
   * Pass each vertex attribute through a map function and retain the
   * original RDD's partitioning and index.
   *
   * @tparam VD2 the type returned by the map function
   *
   * @param f the function applied to each value in the RDD
   * @return a new VertexRDD with values obtained by applying `f` to
   * each of the entries in the original VertexSet.  The resulting
   * VertexRDD retains the same index.
   */
  def mapValues[VD2: ClassManifest](f: (Vid, VD) => VD2): VertexRDD[VD2] =
    this.mapVertexPartitions(_.map(f))

  /**
   * Inner join this VertexSet with another VertexSet which has the
   * same Index.  This function will fail if both VertexSets do not
   * share the same index.  The resulting vertex set will only contain
   * vertices that are in both this and the other vertex set.
   *
   * @tparam VD2 the attribute type of the other VertexSet
   * @tparam VD3 the attribute type of the resulting VertexSet
   *
   * @param other the other VertexSet with which to join.
   * @param f the function mapping a vertex id and its attributes in
   * this and the other vertex set to a new vertex attribute.
   * @return a VertexRDD containing only the vertices in both this
   * and the other VertexSet and with tuple attributes.
   *
   */
  def zipJoin[VD2: ClassManifest, VD3: ClassManifest]
      (other: VertexRDD[VD2])(f: (Vid, VD, VD2) => VD3): VertexRDD[VD3] =
  {
    this.zipVertexPartitions(other) { (thisPart, otherPart) =>
      thisPart.join(otherPart)(f)
    }
  }

  def deltaJoin[VD2: ClassManifest]
      (other: VertexRDD[VD2])(f: (Vid, VD, VD2) => VD): VertexRDD[VD] =
  {
    this.zipVertexPartitions(other) { (thisPart, otherPart) =>
      thisPart.deltaJoin(otherPart)(f)
    }
  }

  /**
   * Left join this VertexSet with another VertexSet which has the
   * same Index.  This function will fail if both VertexSets do not
   * share the same index.  The resulting vertex set contains an entry
   * for each vertex in this set.  If the other VertexSet is missing
   * any vertex in this VertexSet then a `None` attribute is generated
   *
   * @tparam VD2 the attribute type of the other VertexSet
   * @tparam VD3 the attribute type of the resulting VertexSet
   *
   * @param other the other VertexSet with which to join.
   * @param f the function mapping a vertex id and its attributes in
   * this and the other vertex set to a new vertex attribute.
   * @return a VertexRDD containing all the vertices in this
   * VertexSet with `None` attributes used for Vertices missing in the
   * other VertexSet.
   *
   */
  def leftZipJoin[VD2: ClassManifest, VD3: ClassManifest]
      (other: VertexRDD[VD2])(f: (Vid, VD, Option[VD2]) => VD3): VertexRDD[VD3] =
  {
    this.zipVertexPartitions(other) { (thisPart, otherPart) =>
      thisPart.leftJoin(otherPart)(f)
    }
  }

  /**
   * Left join this VertexSet with an RDD containing vertex attribute
   * pairs.  If the other RDD is backed by a VertexSet with the same
   * index than the efficient leftZipJoin implementation is used.  The
   * resulting vertex set contains an entry for each vertex in this
   * set.  If the other VertexSet is missing any vertex in this
   * VertexSet then a `None` attribute is generated.
   *
   * If there are duplicates, the vertex is picked at random.
   *
   * @tparam VD2 the attribute type of the other VertexSet
   * @tparam VD3 the attribute type of the resulting VertexSet
   *
   * @param other the other VertexSet with which to join.
   * @param f the function mapping a vertex id and its attributes in
   * this and the other vertex set to a new vertex attribute.
   * @return a VertexRDD containing all the vertices in this
   * VertexSet with the attribute emitted by f.
   */
  def leftJoin[VD2: ClassManifest, VD3: ClassManifest]
      (other: RDD[(Vid, VD2)])
      (f: (Vid, VD, Option[VD2]) => VD3)
    : VertexRDD[VD3] =
  {
    // Test if the other vertex is a VertexRDD to choose the optimal join strategy.
    // If the other set is a VertexRDD then we use the much more efficient leftZipJoin
    other match {
      case other: VertexRDD[_] =>
        leftZipJoin(other)(f)
      case _ =>
        new VertexRDD[VD3](
          partitionsRDD.zipPartitions(
            other.partitionBy(this.partitioner.get), preservesPartitioning = true)
          { (part, msgs) =>
            val vertexPartition: VertexPartition[VD] = part.next()
            Iterator(vertexPartition.leftJoin(msgs)(f))
          }
        )
    }
  }

  def aggregateUsingIndex[VD2: ClassManifest](
      messages: RDD[(Vid, VD2)], reduceFunc: (VD2, VD2) => VD2): VertexRDD[VD2] =
  {
    val shuffled = MsgRDDFunctions.partitionForAggregation(messages, this.partitioner.get)
    val parts = partitionsRDD.zipPartitions(shuffled, true) { (thisIter, msgIter) =>
      val vertextPartition: VertexPartition[VD] = thisIter.next()
      Iterator(vertextPartition.aggregateUsingIndex(msgIter, reduceFunc))
    }
    new VertexRDD[VD2](parts)
  }

} // end of VertexRDD


/**
 * The VertexRDD singleton is used to construct VertexSets
 */
object VertexRDD {

  /**
   * Construct a vertex set from an RDD of vertex-attribute pairs.
   * Duplicate entries are removed arbitrarily.
   *
   * @tparam VD the vertex attribute type
   *
   * @param rdd the collection of vertex-attribute pairs
   */
  def apply[VD: ClassManifest](rdd: RDD[(Vid, VD)]): VertexRDD[VD] = {
    val partitioned: RDD[(Vid, VD)] = rdd.partitioner match {
      case Some(p) => rdd
      case None => rdd.partitionBy(new HashPartitioner(rdd.partitions.size))
    }
    val vertexPartitions = partitioned.mapPartitions(
      iter => Iterator(VertexPartition(iter)),
      preservesPartitioning = true)
    new VertexRDD(vertexPartitions)
  }

  /**
   * Construct a vertex set from an RDD of vertex-attribute pairs.
   * Duplicate entries are merged using mergeFunc.
   *
   * @tparam VD the vertex attribute type
   *
   * @param rdd the collection of vertex-attribute pairs
   * @param mergeFunc the associative, commutative merge function.
   */
  def apply[VD: ClassManifest](rdd: RDD[(Vid, VD)], mergeFunc: (VD, VD) => VD): VertexRDD[VD] =
  {
    val partitioned: RDD[(Vid, VD)] = rdd.partitioner match {
      case Some(p) => rdd
      case None => rdd.partitionBy(new HashPartitioner(rdd.partitions.size))
    }
    val vertexPartitions = partitioned.mapPartitions(
      iter => Iterator(VertexPartition(iter)),
      preservesPartitioning = true)
    new VertexRDD(vertexPartitions)
  }

  def apply[VD: ClassManifest](vids: RDD[Vid], rdd: RDD[(Vid, VD)], defaultVal: VD)
    : VertexRDD[VD] =
  {
    VertexRDD(vids.map(vid => (vid, defaultVal))).leftJoin(rdd) { (vid, default, value) =>
      value.getOrElse(default)
    }
  }
}
