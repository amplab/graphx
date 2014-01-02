package org.apache.spark.graph.algorithms

import org.scalatest.FunSuite

import org.apache.spark.SparkContext
import org.apache.spark.SparkContext._
import org.apache.spark.graph._
import org.apache.spark.graph.util.GraphGenerators
import org.apache.spark.rdd._


class KcoreSuite extends FunSuite with LocalSparkContext {

  def createTriple(sid: Vid, did: Vid, sattr: Int, dattr: Int, eattr: Int): EdgeTriplet[Int,Int] = {
    val et = new EdgeTriplet[Int,Int]
    et.srcId = sid
    et.dstId = did
    et.srcAttr = sattr
    et.dstAttr = dattr
    et.attr = eattr
    et
  }

  def createKCoreEdges(): Seq[Edge[Int]] = {
    Seq(Edge(11,31), Edge(12,31), Edge(31,33), Edge(31,32), Edge(31,34), Edge(33,34),
        Edge(33,32), Edge(34,32), Edge(32,13), Edge(32,23), Edge(34,23), Edge(23,14),
        Edge(34,21), Edge(34,22), Edge(21,22))
  }

  test("K-Core") {
    withSpark { sc =>
      val rawEdges = createKCoreEdges()
      val vertices = Set((11, 1), (12,1), (13,1), (14,1), (21,2), (22,2), (23,2), (31, 3), (32,3), (33,3), (34,3))
      val graph = Graph.fromEdges(sc.parallelize(rawEdges))
      val resultGraph = Kcore.run(graph, 1, 3)
      val resultVerts = resultGraph.vertices.collect.toSet
      assert(resultVerts === vertices)

    }
  }



}
