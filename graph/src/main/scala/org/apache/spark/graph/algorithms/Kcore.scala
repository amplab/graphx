package org.apache.spark.graph.algorithms

import org.apache.spark.graph._
import scala.math._


object Kcore extends Logging {

  def run[VD: Manifest, ED: Manifest](
      graph: Graph[VD, ED],
      kmin: Int = 1,
      kmax: Int)
    : Graph[Int, ED] = {

    // Graph[(Int, Boolean), ED] - boolean indicates whether it is active or not
    var g = graph.outerJoinVertices(graph.degrees)((vid, oldData, newData) => (newData.getOrElse(0), True))


    var curK = 1
    while (curK <= kmax) {
      g = computeCurrentKCore(g, curK)
      // all vertices still on are part of curK-core, all vertices turned off are members of the shell
      // that their vertex data indicates
      curK += 1
    }
  }

  // TODO(crankshaw) - optionally we can do some post-processing here:
  //  + removes all vertices that are members of a shell x s.t. x < kmin (simple subgraph) 
  //  + change vertex data for all vertices still on to be kmax
  //  + change VD type from (Int, Boolean) to Int


  def computeCurrentKCore(graph: Graph[(Int, Boolean), ED], k: Int) {
    def sendMsg(et: EdgeTriplet[(Int, Boolean), ED]): Iterator[(Vid, (Int, Boolean))] = {
      // check if either vertex has already been turned off, in which case we do nothing
      if !(et.srcAttr._2 || et.dstAttr._2) {
        // TODO(crankshaw) - need this case for the first round of Pregel for k > 1
        Iterator.empty
      } else if (et.srcAttr._1 < k && et.dstAttr._1 < k) {
        // tell both vertices to turn off but don't need change count value
        Iterator((et.srcId, (0, false)), (et.dstId, (0, false)))
      } else if (et.srcAttr._1 < k) {
        // if src is being pruned, tell dst to subtract from vertex count but don't turn off
        Iterator((et.srcId, (0, false)), (et.dstId, (1, true)))
      } else if (et.dstAttr._1 < k) {
        // if dst is being pruned, tell src to subtract from vertex count but don't turn off
        Iterator((et.dstId, (0, false)), (et.srcId, (1, true)))
      } else {
        // no changes required
        Iterator.empty
      }
    }

    // subtracts removed neighbors from neighbor count and tells vertex whether it was turned off or not
    def mergeMsg(m1: (Int, Boolean), m2: (Int, Boolean)): (Int, Boolean) = {
      // val count = (max(k - 1, m1._1 + m2._1))
      // val on = m1._2 + m2._2
      // (count, on)

      // because the vertex count doesn't change within a round, all edgetriplets for which
      // this vertex is part of should either tell the vertex to be on or off, so the messages
      // should indicate the same state
      if (m1._2 != m2._2) {
        logWarning("KCORE: merged messages indicate different vertex states")
      }

      // compute number of removed neighbors and determine whether vertex is on or off
      (m1._1 + m2._1, m1._2 && m2._2)
    }

    def vProg(vid: Vid, data: (Int, Boolean), update: (Int, Boolean)): (Int, Boolean) {
      // If the count drops below k, then the vertex will be turned off in the next round anyway
      // This *should* make sure that if the vertex has been turned off, then it will be recorded as
      // having been turned off when pruning the kth shell, and thus at the end we know the highest kcore
      // it was part of was for k-1
      val newCount = max(k - 1, data._1 - update._1)
      val on = data._2 && update._2
      (newCount, on)
    }

    // Note that initial message should have no effect
    Pregel.undirectedRun(graph, (0, True), vprog, sendMsg, mergeMsg)
  }

}
