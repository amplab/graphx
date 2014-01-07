package org.apache.spark.graph.algorithms

import org.apache.spark.graph._
import com.twitter.algebird.{ Min, Monoid }

object ShortestPaths {
  type SPMap = Map[Vid, Min[Int]] // map of landmarks -> minimum distance to landmark
  def SPMap(x: (Vid, Min[Int])*) = Map(x: _*)
  def increment(spmap: SPMap): SPMap = spmap.map { case (v, Min(d)) => v -> Min(d + 1) }

  /**
   * Compute the shortest paths to each landmark for each vertex and
   * return an RDD with the map of landmarks to their shortest-path
   * lengths.
   *
   * @tparam VD the shortest paths map for the vertex
   * @tparam ED the incremented shortest-paths map of the originating
   * vertex (discarded in the computation)
   *
   * @param graph the graph for which to compute the shortest paths
   * @param landmarks the list of landmark vertex ids
   *
   * @return a graph with vertex attributes containing a map of the
   * shortest paths to each landmark
   */
  def run[VD, ED](graph: Graph[VD, ED], landmarks: Seq[Vid])
    (implicit m1: Manifest[VD], m2: Manifest[ED], spMapMonoid: Monoid[SPMap]): Graph[SPMap, SPMap] = {

    val spGraph = graph
      .mapVertices{ (vid, attr) =>
        if (landmarks.contains(vid)) SPMap(vid -> Min(0))
        else SPMap()
      }
      .mapTriplets{ edge => edge.srcAttr }

    val initialMessage = SPMap()

    def vertexProgram(id: Vid, attr: SPMap, msg: SPMap): SPMap = {
      spMapMonoid.plus(attr, msg)
    }

    def sendMessage(edge: EdgeTriplet[SPMap, SPMap]): Iterator[(Vid, SPMap)] = {
      val newAttr = increment(edge.srcAttr)
      if (edge.dstAttr != spMapMonoid.plus(newAttr, edge.dstAttr)) Iterator((edge.dstId, newAttr))
      else Iterator.empty
    }

    def messageCombiner(s1: SPMap, s2: SPMap): SPMap = {
      spMapMonoid.plus(s1, s2)
    }

    Pregel(spGraph, initialMessage)(
      vertexProgram, sendMessage, messageCombiner)
  }

}
