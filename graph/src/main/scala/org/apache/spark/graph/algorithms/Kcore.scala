package org.apache.spark.graph.algorithms

import org.apache.spark.graph._
import org.apache.spark._
import scala.math._

object KCore extends Logging {
  /**
   * Compute the k-core decomposition of the graph for all k <= kmax. This
   * uses the iterative pruning algorithm discussed by Alvarez-Hamelin et al.
   * in K-Core Decomposition: a Tool For the Visualization of Large Scale Networks
   * (see <a href="http://arxiv.org/abs/cs/0504107">http://arxiv.org/abs/cs/0504107</a>).
   *
   * @tparam VD the vertex attribute type (discarded in the computation)
   * @tparam ED the edge attribute type (preserved in the computation)
   *
   * @param graph the graph for which to compute the connected components
   * @param kmax the maximum value of k to decompose the graph
   *
   * @return a graph where the vertex attribute is the minimum of
   * kmax or the highest value k for which that vertex was a member of
   * the k-core.
   *
   * @note This method has the advantage of returning not just a single kcore of the
   * graph but will yield all the cores for all k in [1, kmax].
   */

  def run[VD: Manifest, ED: Manifest](
      graph: Graph[VD, ED],
      kmax: Int)
    : Graph[Int, ED] = {

    // Graph[(Int, Boolean), ED] - boolean indicates whether it is active or not
    var g = graph.outerJoinVertices(graph.degrees)((vid, oldData, newData) => (newData.getOrElse(0), true))
    var curK = 1
    while (curK <= kmax) {
      g = computeCurrentKCore(g, curK)
      curK += 1
    }
    g.mapVertices({ case (_, (k, _)) => k})
  }

  def computeCurrentKCore[ED: Manifest](graph: Graph[(Int, Boolean), ED], k: Int) = {
    def sendMsg(et: EdgeTriplet[(Int, Boolean), ED]): Iterator[(Vid, (Int, Boolean))] = {
      if (!et.srcAttr._2 || !et.dstAttr._2) {
        // if either vertex has already been turned off, in which case we do nothing
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
        Iterator.empty
      }
    }

    // subtracts removed neighbors from neighbor count and tells vertex whether it was turned off or not
    def mergeMsg(m1: (Int, Boolean), m2: (Int, Boolean)): (Int, Boolean) = {
      (m1._1 + m2._1, m1._2 && m2._2)
    }

    def vProg(vid: Vid, data: (Int, Boolean), update: (Int, Boolean)): (Int, Boolean) = {
      var newCount = data._1
      var on = data._2
      if (on) {
        newCount = max(k - 1, data._1 - update._1)
        on = update._2
      }
      (newCount, on)
    }

    // Note that initial message should have no effect
    Pregel.undirectedRun(graph, (0, true))(vProg, sendMsg, mergeMsg)
  }
}
