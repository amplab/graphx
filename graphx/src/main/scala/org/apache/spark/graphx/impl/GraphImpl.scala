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

import org.apache.spark.Logging
import org.apache.spark.util.collection.PrimitiveVector
import org.apache.spark.{HashPartitioner, Partitioner}
import org.apache.spark.SparkContext._
import org.apache.spark.graphx._
import org.apache.spark.graphx.impl.GraphImpl._
import org.apache.spark.graphx.impl.MsgRDDFunctions._
import org.apache.spark.graphx.util.BytecodeUtils
import org.apache.spark.rdd.{ShuffledRDD, RDD}
import org.apache.spark.storage.StorageLevel
import org.apache.spark.util.ClosureCleaner


/**
 * A graph that supports computation on graphs.
 *
 * Graphs are represented using two classes of data: vertex-partitioned and
 * edge-partitioned. `vertices` contains vertex attributes, which are vertex-partitioned. `edges`
 * contains edge attributes, which are edge-partitioned. For operations on vertex neighborhoods,
 * vertex attributes are replicated to the edge partitions where they appear as sources or
 * destinations. `routingTable` stores the routing information for shipping vertex attributes to
 * edge partitions. `replicatedVertexView` stores a view of the replicated vertex attributes created
 * using the routing table.
 */
class GraphImpl[VD: ClassTag, ED: ClassTag] protected (
    @transient val vertices: VertexRDD[VD],
    @transient private val replicatedVertexView: ReplicatedVertexView[VD, ED])
  extends Graph[VD, ED] with Serializable with Logging {

  /** Default constructor is provided to support serialization */
  protected def this() = this(null, null)

  @transient override val edges: EdgeRDD[ED, VD] = replicatedVertexView.edges

  /** Return a RDD that brings edges together with their source and destination vertices. */
  @transient override lazy val triplets: RDD[EdgeTriplet[VD, ED]] = {
    replicatedVertexView.upgrade(vertices, true, true)
    replicatedVertexView.edges.partitionsRDD.mapPartitions(_.flatMap {
      case (pid, part) => part.tripletIterator()
    })
  }

  override def persist(newLevel: StorageLevel): Graph[VD, ED] = {
    vertices.persist(newLevel)
    replicatedVertexView.edges.persist(newLevel)
    this
  }

  override def cache(): Graph[VD, ED] = persist(StorageLevel.MEMORY_ONLY)

  override def unpersistVertices(blocking: Boolean = true): Graph[VD, ED] = {
    vertices.unpersist(blocking)
    this
  }

  override def partitionBy(partitionStrategy: PartitionStrategy): Graph[VD, ED] = {
    val numPartitions = replicatedVertexView.edges.partitions.size
    val edTag = classTag[ED]
    val vdTag = classTag[VD]
    val newEdges = new EdgeRDD(replicatedVertexView.edges.map { e =>
      val part: PartitionID = partitionStrategy.getPartition(e.srcId, e.dstId, numPartitions)

      // Should we be using 3-tuple or an optimized class
      new MessageToPartition(part, (e.srcId, e.dstId, e.attr))
    }
      .partitionBy(new HashPartitioner(numPartitions))
      .mapPartitionsWithIndex( { (pid, iter) =>
        val builder = new EdgePartitionBuilder[ED, VD]()(edTag, vdTag)
        iter.foreach { message =>
          val data = message.data
          builder.add(data._1, data._2, data._3)
        }
        val edgePartition = builder.toEdgePartition
        Iterator((pid, edgePartition))
      }, preservesPartitioning = true))
    GraphImpl.fromExistingRDDs(vertices, newEdges)
  }

  override def reverse: Graph[VD, ED] = {
    val newEdges = edges.mapEdgePartitions((pid, part) => part.reverse)
    new GraphImpl(vertices, replicatedVertexView.withEdges(newEdges))
  }

  override def mapVertices[VD2: ClassTag](f: (VertexId, VD) => VD2): Graph[VD2, ED] = {
    if (classTag[VD] equals classTag[VD2]) {
      // The map preserves type, so we can use incremental replication
      val newVerts = vertices.mapVertexPartitions(_.map(f)).cache()
      val changedVerts = vertices.asInstanceOf[VertexRDD[VD2]].diff(newVerts)
      val newReplicatedVertexView = replicatedVertexView.asInstanceOf[ReplicatedVertexView[VD2, ED]]
        .updateVertices(changedVerts)
      new GraphImpl(newVerts, newReplicatedVertexView)
    } else {
      // The map does not preserve type, so we must re-replicate all vertices
      GraphImpl(vertices.mapVertexPartitions(_.map(f)), replicatedVertexView.edges)
    }
  }

  override def mapEdges[ED2: ClassTag](
      f: (PartitionID, Iterator[Edge[ED]]) => Iterator[ED2]): Graph[VD, ED2] = {
    val newEdges = replicatedVertexView.edges
      .mapEdgePartitions((pid, part) => part.map(f(pid, part.iterator)))
    new GraphImpl(vertices, replicatedVertexView.withEdges(newEdges))
  }

  override def mapTriplets[ED2: ClassTag](
      f: (PartitionID, Iterator[EdgeTriplet[VD, ED]]) => Iterator[ED2]): Graph[VD, ED2] = {
    val mapUsesSrcAttr = accessesVertexAttr(f, "srcAttr")
    val mapUsesDstAttr = accessesVertexAttr(f, "dstAttr")
    replicatedVertexView.upgrade(vertices, mapUsesSrcAttr, mapUsesDstAttr)
    val newEdges = replicatedVertexView.edges.mapEdgePartitions { (pid, part) =>
      part.map(f(pid, part.tripletIterator(mapUsesSrcAttr, mapUsesDstAttr)))
    }
    new GraphImpl(vertices, replicatedVertexView.withEdges(newEdges))
  }

  override def subgraph(
      epred: EdgeTriplet[VD, ED] => Boolean = x => true,
      vpred: (VertexId, VD) => Boolean = (a, b) => true): Graph[VD, ED] = {
    // Filter the vertices, reusing the partitioner and the index from this graph
    val newVerts = vertices.mapVertexPartitions(_.filter(vpred))
    // Filter the triplets
    replicatedVertexView.upgrade(vertices, true, true)
    val newEdges = replicatedVertexView.edges.filter(epred, vpred)
    new GraphImpl(newVerts, replicatedVertexView.withEdges(newEdges))
  }

  override def mask[VD2: ClassTag, ED2: ClassTag] (
      other: Graph[VD2, ED2]): Graph[VD, ED] = {
    val newVerts = vertices.innerJoin(other.vertices) { (vid, v, w) => v }
    val newEdges = replicatedVertexView.edges.innerJoin(other.edges) { (src, dst, v, w) => v }
    new GraphImpl(newVerts, replicatedVertexView.withEdges(newEdges))
  }

  override def groupEdges(merge: (ED, ED) => ED): Graph[VD, ED] = {
    val newEdges = replicatedVertexView.edges.mapEdgePartitions(
      (pid, part) => part.groupEdges(merge))
    new GraphImpl(vertices, replicatedVertexView.withEdges(newEdges))
  }

  // ///////////////////////////////////////////////////////////////////////////////////////////////
  // Lower level transformation methods
  // ///////////////////////////////////////////////////////////////////////////////////////////////

  override def mapReduceTriplets[A: ClassTag](
      mapFunc: EdgeTriplet[VD, ED] => Iterator[(VertexId, A)],
      reduceFunc: (A, A) => A,
      activeSetOpt: Option[(VertexRDD[_], EdgeDirection)] = None): VertexRDD[A] = {

    vertices.cache()

    // For each vertex, replicate its attribute only to partitions where it is
    // in the relevant position in an edge.
    val mapUsesSrcAttr = accessesVertexAttr(mapFunc, "srcAttr")
    val mapUsesDstAttr = accessesVertexAttr(mapFunc, "dstAttr")
    replicatedVertexView.upgrade(vertices, mapUsesSrcAttr, mapUsesDstAttr)
    val view = activeSetOpt match {
      case Some((activeSet, _)) =>
        replicatedVertexView.withActiveSet(activeSet)
      case None =>
        replicatedVertexView
    }
    val activeDirectionOpt = activeSetOpt.map(_._2)

    // Map and combine.
    val preAgg = view.edges.partitionsRDD.mapPartitions(_.flatMap {
      case (pid, edgePartition) =>
        // Choose scan method
        val activeFraction = edgePartition.numActives.getOrElse(0) / edgePartition.indexSize.toFloat
        val edgeIter = activeDirectionOpt match {
          case Some(EdgeDirection.Both) =>
            if (activeFraction < 0.8) {
              edgePartition.indexIterator(srcVertexId => edgePartition.isActive(srcVertexId))
                .filter(e => edgePartition.isActive(e.dstId))
            } else {
              edgePartition.iterator.filter(e =>
                edgePartition.isActive(e.srcId) && edgePartition.isActive(e.dstId))
            }
          case Some(EdgeDirection.Either) =>
            // TODO: Because we only have a clustered index on the source vertex ID, we can't filter
            // the index here. Instead we have to scan all edges and then do the filter.
            edgePartition.iterator.filter(e =>
              edgePartition.isActive(e.srcId) || edgePartition.isActive(e.dstId))
          case Some(EdgeDirection.Out) =>
            if (activeFraction < 0.8) {
              edgePartition.indexIterator(srcVertexId => edgePartition.isActive(srcVertexId))
            } else {
              edgePartition.iterator.filter(e => edgePartition.isActive(e.srcId))
            }
          case Some(EdgeDirection.In) =>
            edgePartition.iterator.filter(e => edgePartition.isActive(e.dstId))
          case _ => // None
            edgePartition.iterator
        }

        // Scan edges and run the map function
        val mapOutputs = edgePartition.upgradeIterator(edgeIter, mapUsesSrcAttr, mapUsesDstAttr)
          .flatMap(mapFunc(_))
        // Note: This doesn't allow users to send messages to arbitrary vertices.
        edgePartition.vertexPartition.aggregateUsingIndex(mapOutputs, reduceFunc).iterator
    }).setName("GraphImpl.mapReduceTriplets - preAgg")

    // do the final reduction reusing the index map
    vertices.aggregateUsingIndex(preAgg, reduceFunc)
  } // end of mapReduceTriplets

  override def outerJoinVertices[U: ClassTag, VD2: ClassTag]
      (other: RDD[(VertexId, U)])
      (updateF: (VertexId, VD, Option[U]) => VD2): Graph[VD2, ED] = {
    if (classTag[VD] equals classTag[VD2]) {
      // updateF preserves type, so we can use incremental replication
      val newVerts = vertices.leftJoin(other)(updateF)
      val changedVerts = vertices.asInstanceOf[VertexRDD[VD2]].diff(newVerts)
      val newReplicatedVertexView =
        replicatedVertexView.asInstanceOf[ReplicatedVertexView[VD2, ED]].updateVertices(changedVerts)
      new GraphImpl(newVerts, newReplicatedVertexView)
    } else {
      // updateF does not preserve type, so we must re-replicate all vertices
      val newVerts = vertices.leftJoin(other)(updateF)
      GraphImpl(newVerts, replicatedVertexView.edges)
    }
  }

  /** Test whether the closure accesses the the attribute with name `attrName`. */
  private def accessesVertexAttr(closure: AnyRef, attrName: String): Boolean = {
    try {
      BytecodeUtils.invokedMethod(closure, classOf[EdgeTriplet[VD, ED]], attrName)
    } catch {
      case _: ClassNotFoundException => true // if we don't know, be conservative
    }
  }
} // end of class GraphImpl


object GraphImpl {

  def apply[VD: ClassTag, ED: ClassTag](
      edges: RDD[Edge[ED]],
      defaultVertexAttr: VD): GraphImpl[VD, ED] = {
    fromEdgeRDD(createEdgeRDD(edges), defaultVertexAttr)
  }

  def fromEdgePartitions[VD: ClassTag, ED: ClassTag](
      edgePartitions: RDD[(PartitionID, EdgePartition[ED, VD])],
      defaultVertexAttr: VD): GraphImpl[VD, ED] = {
    fromEdgeRDD(new EdgeRDD(edgePartitions), defaultVertexAttr)
  }

  def apply[VD: ClassTag, ED: ClassTag](
      vertices: RDD[(VertexId, VD)],
      edges: RDD[Edge[ED]],
      defaultVertexAttr: VD): GraphImpl[VD, ED] = {
    val edgeRDD = createEdgeRDD(edges)(classTag[ED], classTag[VD]).cache()
    val vertexRDD = VertexRDD(vertices, edgeRDD, defaultVertexAttr)
    GraphImpl(vertexRDD, edgeRDD)
  }

  def apply[VD: ClassTag, ED: ClassTag](
      vertices: VertexRDD[VD],
      edges: EdgeRDD[ED, _]): GraphImpl[VD, ED] = {
    // Convert the vertex partitions in edges to the correct type
    val newEdges = edges.mapEdgePartitions(
      (pid, part) => part.withVertexPartition(part.vertexPartition.map(
        (vid, attr) => null.asInstanceOf[VD])))
    GraphImpl.fromExistingRDDs(vertices, newEdges)
  }

  def fromExistingRDDs[VD: ClassTag, ED: ClassTag](
      vertices: VertexRDD[VD],
      edges: EdgeRDD[ED, VD]): GraphImpl[VD, ED] = {
    new GraphImpl(vertices, new ReplicatedVertexView(edges))
  }

  /**
   * Create the edge RDD, which is much more efficient for Java heap storage than the normal edges
   * data structure (RDD[(VertexId, VertexId, ED)]).
   *
   * The edge RDD contains multiple partitions, and each partition contains only one RDD key-value
   * pair: the key is the partition id, and the value is an EdgePartition object containing all the
   * edges in a partition.
   */
  private def createEdgeRDD[ED: ClassTag, VD: ClassTag](
      edges: RDD[Edge[ED]]): EdgeRDD[ED, VD] = {
    val edgePartitions = edges.mapPartitionsWithIndex { (pid, iter) =>
      val builder = new EdgePartitionBuilder[ED, VD]
      iter.foreach { e =>
        builder.add(e.srcId, e.dstId, e.attr)
      }
      Iterator((pid, builder.toEdgePartition))
    }
    new EdgeRDD(edgePartitions)
  }

  private def fromEdgeRDD[VD: ClassTag, ED: ClassTag](
      edges: EdgeRDD[ED, VD],
      defaultVertexAttr: VD): GraphImpl[VD, ED] = {
    edges.cache()
    val vertices = VertexRDD.fromEdges(edges, edges.partitions.size, defaultVertexAttr)
    fromExistingRDDs(vertices, edges)
  }

} // end of object GraphImpl
