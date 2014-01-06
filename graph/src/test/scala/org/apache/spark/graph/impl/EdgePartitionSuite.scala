package org.apache.spark.graph.impl

import scala.util.Random

import org.scalatest.FunSuite

import org.apache.spark.graph._

class EdgePartitionSuite extends FunSuite {

  test("reverse") {
    val edges = List(Edge(0, 1, 0), Edge(1, 2, 0), Edge(2, 0, 0))
    val reversedEdges = List(Edge(0, 2, 0), Edge(1, 0, 0), Edge(2, 1, 0))
    val builder = new EdgePartitionBuilder[Int]
    for (e <- edges) {
      builder.add(e.srcId, e.dstId, e.attr)
    }
    val edgePartition = builder.toEdgePartition
    assert(edgePartition.reverse.iterator.map(_.copy()).toList === reversedEdges)
    assert(edgePartition.reverse.reverse.iterator.map(_.copy()).toList === edges)
  }

  test("map") {
    val edges = List(Edge(0, 1, 0), Edge(1, 2, 0), Edge(2, 0, 0))
    val builder = new EdgePartitionBuilder[Int]
    for (e <- edges) {
      builder.add(e.srcId, e.dstId, e.attr)
    }
    val edgePartition = builder.toEdgePartition
    assert(edgePartition.map(e => e.srcId + e.dstId).iterator.map(_.copy()).toList ===
      edges.map(e => e.copy(attr = e.srcId + e.dstId)))
  }

  test("groupEdges") {
    val edges = List(
      Edge(0, 1, 1), Edge(1, 2, 2), Edge(2, 0, 4), Edge(0, 1, 8), Edge(1, 2, 16), Edge(2, 0, 32))
    val groupedEdges = List(Edge(0, 1, 9), Edge(1, 2, 18), Edge(2, 0, 36))
    val builder = new EdgePartitionBuilder[Int]
    for (e <- edges) {
      builder.add(e.srcId, e.dstId, e.attr)
    }
    val edgePartition = builder.toEdgePartition
    assert(edgePartition.groupEdges(_ + _).iterator.map(_.copy()).toList === groupedEdges)
  }

  test("indexIterator") {
    val edgesFrom0 = List(Edge(0, 1, 0))
    val edgesFrom1 = List(Edge(1, 0, 0), Edge(1, 2, 0))
    val sortedEdges = edgesFrom0 ++ edgesFrom1
    val builder = new EdgePartitionBuilder[Int]
    for (e <- Random.shuffle(sortedEdges)) {
      builder.add(e.srcId, e.dstId, e.attr)
    }

    val edgePartition = builder.toEdgePartition
    assert(edgePartition.iterator.map(_.copy()).toList === sortedEdges)
    assert(edgePartition.indexIterator(_ == 0).map(_.copy()).toList === edgesFrom0)
    assert(edgePartition.indexIterator(_ == 1).map(_.copy()).toList === edgesFrom1)
  }

  test("innerJoin") {
    def makeEdgePartition[A: ClassManifest](xs: Iterable[(Int, Int, A)]): EdgePartition[A] = {
      val builder = new EdgePartitionBuilder[A]
      for ((src, dst, attr) <- xs) { builder.add(src: Vid, dst: Vid, attr) }
      builder.toEdgePartition
    }
    val aList = List((0, 1, 0), (1, 0, 0), (1, 2, 0), (5, 4, 0), (5, 5, 0))
    val bList = List((0, 1, 0), (1, 0, 0), (1, 1, 0), (3, 4, 0), (5, 5, 0))
    val a = makeEdgePartition(aList)
    val b = makeEdgePartition(bList)

    assert(a.innerJoin(b) { (src, dst, a, b) => a }.iterator.map(_.copy()).toList ===
      List(Edge(0, 1, 0), Edge(1, 0, 0), Edge(5, 5, 0)))
  }
}
