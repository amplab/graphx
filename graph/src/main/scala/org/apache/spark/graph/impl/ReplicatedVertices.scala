package org.apache.spark.graph.impl

import org.apache.spark.SparkContext._
import org.apache.spark.rdd.RDD
import org.apache.spark.util.collection.{OpenHashSet, PrimitiveKeyOpenHashMap}

import org.apache.spark.graph._

/**
 * Stores the vertex attribute values after they are replicated.
 */
private[impl]
class ReplicatedVertices[VD: ClassManifest](
    vertices: VertexRDD[VD],
    edges: EdgeRDD[_],
    vertexPlacement: VertexPlacement) {

  val bothAttrs: RDD[(Pid, (VertexIdToIndexMap, Array[VD]))] =
    create(vertices, edges, vertexPlacement, true, true)

  val srcAttrOnly: RDD[(Pid, (VertexIdToIndexMap, Array[VD]))] =
    create(vertices, edges, vertexPlacement, true, false)

  val dstAttrOnly: RDD[(Pid, (VertexIdToIndexMap, Array[VD]))] =
    create(vertices, edges, vertexPlacement, false, true)

  val noAttrs: RDD[(Pid, (VertexIdToIndexMap, Array[VD]))] =
    create(vertices, edges, vertexPlacement, false, false)

  def get(includeSrc: Boolean, includeDst: Boolean): RDD[(Pid, (VertexIdToIndexMap, Array[VD]))] = {
    (includeSrc, includeDst) match {
      case (true, true) => bothAttrs
      case (true, false) => srcAttrOnly
      case (false, true) => dstAttrOnly
      case (false, false) => noAttrs
    }
  }

  def zipWithEdges[ED, U: ClassManifest]
      (edges: EdgeRDD[ED], includeSrc: Boolean, includeDst: Boolean)
      (f: (Pid, EdgePartition[ED], PrimitiveKeyOpenHashMap[Vid, VD]) => Iterator[U]): RDD[U] = {
    val cleanF = vertices.context.clean(f)
    edges.zipEdgePartitions(get(includeSrc, includeDst)) { (edgePartition, vIter) =>
      val (pid, (vidToIndex, vertexArray)) = vIter.next()
      assert(vidToIndex.capacity == vertexArray.size)
      val vmap = new PrimitiveKeyOpenHashMap(vidToIndex, vertexArray)
      cleanF(pid, edgePartition, vmap)
    }
  }

  private def create[VD: ClassManifest](
       vertices: VertexRDD[VD],
       edges: EdgeRDD[_],
       vertexPlacement: VertexPlacement,
       includeSrcAttr: Boolean,
       includeDstAttr: Boolean): RDD[(Pid, (VertexIdToIndexMap, Array[VD]))] = {

    val placement = vertexPlacement.get(includeSrcAttr, includeDstAttr)

    // Send each edge partition the vertex attributes it wants, as specified in
    // vertexPlacement
    val msgsByPartition = placement.zipPartitions(vertices.partitionsRDD) {
      (pid2vidIter, vertexPartIter) =>
        val pid2vid: Array[Array[Vid]] = pid2vidIter.next()
        val vertexPart: VertexPartition[VD] = vertexPartIter.next()
        val output = new Array[(Pid, VertexAttributeBlock[VD])](pid2vid.size)
        for (pid <- 0 until pid2vid.size) {
          val block = new VertexAttributeBlock(
            pid2vid(pid), pid2vid(pid).map(vid => vertexPart(vid)))
          output(pid) = (pid, block)
        }
        output.iterator
    }.partitionBy(edges.partitioner.get).cache()
    // TODO: Consider using a specialized shuffler.

    // Within each edge partition, create a local map from vid to an index into
    // the attribute array. Each map contains a superset of the vertices that it
    // will receive, because it stores vids from both the source and destination
    // of edges. It must always include both source and destination vids because
    // some operations, such as GraphImpl.mapReduceTriplets, rely on this.
    val localVidMap = edges.partitionsRDD.mapPartitions(_.map {
      case (pid, epart) =>
        val vidToIndex = new VertexIdToIndexMap
        epart.foreach { e =>
          vidToIndex.add(e.srcId)
          vidToIndex.add(e.dstId)
        }
        (pid, vidToIndex)
    }, preservesPartitioning = true).cache()

    // Within each edge partition, place the vertex attributes received from
    // msgsByPartition into the correct locations specified in localVidMap
    localVidMap.zipPartitions(msgsByPartition) {
      (mapIter, msgsIter) =>
        val (pid, vidToIndex) = mapIter.next()
        assert(!mapIter.hasNext)
        // Populate the vertex array using the vidToIndex map
        val vertexArray = new Array[VD](vidToIndex.capacity)
        for ((_, block) <- msgsIter) {
          for (i <- 0 until block.vids.size) {
            val vid = block.vids(i)
            val attr = block.attrs(i)
            val ind = vidToIndex.getPos(vid) & OpenHashSet.POSITION_MASK
            vertexArray(ind) = attr
          }
        }
        Iterator((pid, (vidToIndex, vertexArray)))
    }.cache()
  }

}

class VertexAttributeBlock[VD: ClassManifest](val vids: Array[Vid], val attrs: Array[VD])
