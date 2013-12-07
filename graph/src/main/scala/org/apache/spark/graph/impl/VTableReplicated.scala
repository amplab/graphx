package org.apache.spark.graph.impl

import org.apache.spark.SparkContext._
import org.apache.spark.rdd.RDD
import org.apache.spark.util.collection.PrimitiveKeyOpenHashMap
import org.apache.spark.util.collection.{PrimitiveVector, OpenHashSet}

import org.apache.spark.graph._

/**
 * Stores the vertex attribute values after they are replicated.
 */
private[impl]
class VTableReplicated[VD: ClassManifest](
    vTable: VertexRDD[VD],
    eTable: EdgeRDD[_],
    vertexPlacement: VertexPlacement,
    prevVTableReplicated: Option[VTableReplicated[VD]] = None) {

  val bothAttrs: RDD[(Pid, VertexPartition[VD])] =
    createVTableReplicated(vTable, eTable, vertexPlacement, true, true)

  val srcAttrOnly: RDD[(Pid, VertexPartition[VD])] =
    createVTableReplicated(vTable, eTable, vertexPlacement, true, false)

  val dstAttrOnly: RDD[(Pid, VertexPartition[VD])] =
    createVTableReplicated(vTable, eTable, vertexPlacement, false, true)

  val noAttrs: RDD[(Pid, VertexPartition[VD])] =
    createVTableReplicated(vTable, eTable, vertexPlacement, false, false)

  def get(includeSrc: Boolean, includeDst: Boolean): RDD[(Pid, VertexPartition[VD])] = {
    (includeSrc, includeDst) match {
      case (true, true) => bothAttrs
      case (true, false) => srcAttrOnly
      case (false, true) => dstAttrOnly
      case (false, false) => noAttrs
    }
  }

  private def createVTableReplicated(
       vTable: VertexRDD[VD],
       eTable: EdgeRDD[_],
       vertexPlacement: VertexPlacement,
       includeSrcAttr: Boolean,
       includeDstAttr: Boolean): RDD[(Pid, VertexPartition[VD])] = {

    val placement = vertexPlacement.get(includeSrcAttr, includeDstAttr)
    val vdManifest = classManifest[VD]

    // Send each edge partition the vertex attributes it wants, as specified in
    // vertexPlacement
    val msgsByPartition = placement.zipPartitions(vTable.partitionsRDD)(VTableReplicated.buildBuffer(_, _)(vdManifest))
      .partitionBy(eTable.partitioner.get).cache()
    // TODO: Consider using a specialized shuffler.

    prevVTableReplicated match {
      case Some(vTableReplicated) =>
        val prev: RDD[(Pid, VertexPartition[VD])] =
          vTableReplicated.get(includeSrcAttr, includeDstAttr)

        prev.zipPartitions(msgsByPartition) { (vTableIter, msgsIter) =>
          val (pid, vertexPartition) = vTableIter.next()
          val newVPart = vertexPartition.updateUsingIndex(msgsIter.flatMap(_._2.iterator))(vdManifest)
          Iterator((pid, newVPart))
        }.cache()

      case None =>
        // Within each edge partition, create a local map from vid to an index into
        // the attribute array. Each map contains a superset of the vertices that it
        // will receive, because it stores vids from both the source and destination
        // of edges. It must always include both source and destination vids because
        // some operations, such as GraphImpl.mapReduceTriplets, rely on this.
        val localVidMap = eTable.partitionsRDD.mapPartitions(_.map {
          case (pid, epart) =>
            val vidToIndex = new VertexIdToIndexMap
            val vidToSrcEdgePosition = new PrimitiveKeyOpenHashMap[Long, Int]
            assert(epart.sorted)
            // println("Generating localVidMap for partition %d".format(pid))
            var currentSrcId: Vid = -1
            var i = 0
            epart.foreach { e =>
              if (e.srcId != currentSrcId || i == 0) {
                // println("  [%d] Edge (%d, %d) is the start of a new block".format(i, e.srcId, e.dstId))
                currentSrcId = e.srcId
                vidToSrcEdgePosition.update(e.srcId, i)
              } else {
                // println("  [%d] Edge (%d, %d)".format(i, e.srcId, e.dstId))
              }
              vidToIndex.add(e.srcId)
              vidToIndex.add(e.dstId)
              i += 1
            }
            (pid, (vidToIndex, vidToSrcEdgePosition))
        }, preservesPartitioning = true).cache()

        // Within each edge partition, place the vertex attributes received from
        // msgsByPartition into the correct locations specified in localVidMap
        localVidMap.zipPartitions(msgsByPartition) { (mapIter, msgsIter) =>
          val (pid, (vidToIndex, vidToSrcEdgePosition)) = mapIter.next()
          assert(!mapIter.hasNext)
          // Populate the vertex array using the vidToIndex map
          val vertexArray = vdManifest.newArray(vidToIndex.capacity)
          for ((_, block) <- msgsIter) {
            for (i <- 0 until block.vids.size) {
              val vid = block.vids(i)
              val attr = block.attrs(i)
              val ind = vidToIndex.getPos(vid)
              vertexArray(ind) = attr
            }
          }
          // Populate the map from vid to source edge position in the edge partition
          // println("Generating vTableReplicated for partition %d".format(pid))
          // println("vidToSrcEdgePosition:")
          // vidToSrcEdgePosition.iterator.foreach(pair => println("  " + pair))
          val srcEdgePositions = Array.fill(vidToIndex.capacity)(-1)
          vidToSrcEdgePosition.iterator.foreach {
            case (vid, srcEdgePosition) =>
              if (vidToIndex.getPos(vid) != -1) {
                val ind = vidToIndex.getPos(vid)
                // println("  Inserting edge index for vertex %d (local index %d) --> %d".format(vid, ind, srcEdgePosition))
                srcEdgePositions(ind) = srcEdgePosition
              }
          }
          Iterator((pid, new VertexPartition(
            vidToIndex, vertexArray, vidToIndex.getBitSet, srcEdgePositions)(vdManifest)))
        }.cache()
    }
  }

}


object VTableReplicated {

  def buildBuffer[VD: ClassManifest](pid2vidIter: Iterator[Array[Array[Vid]]], vertexPartIter: Iterator[VertexPartition[VD]]) = {
    val pid2vid: Array[Array[Vid]] = pid2vidIter.next()
    val vertexPart: VertexPartition[VD] = vertexPartIter.next()

    Iterator.tabulate(pid2vid.size) { pid =>
      val vidsCandidate = pid2vid(pid)
      val size = vidsCandidate.length
      val vids = new PrimitiveVector[Vid](pid2vid(pid).size)
      val attrs = new PrimitiveVector[VD](pid2vid(pid).size)
      var i = 0
      while (i < size) {
        val vid = vidsCandidate(i)
        if (vertexPart.isDefined(vid)) {
          vids += vid
          attrs += vertexPart(vid)
        }
        i += 1
      }
      (pid, new VertexAttributeBlock(vids.trim().array, attrs.trim().array))
    }
  }
}

class VertexAttributeBlock[VD: ClassManifest](val vids: Array[Vid], val attrs: Array[VD]) {
  def iterator: Iterator[(Vid, VD)] = (0 until vids.size).iterator.map { i => (vids(i), attrs(i)) }
}
