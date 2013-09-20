package org.apache.spark.graph

import org.scalatest.FunSuite

import org.apache.spark.SparkContext
import org.apache.spark.graph._



class GraphSuite extends FunSuite with LocalSparkContext {

//  val sc = new SparkContext("local[4]", "test")

  test("Graph Creation") {
    val rawEdges = (0L to 100L).zip((1L to 99L) :+ 0L)
    val edges = sc.parallelize(rawEdges)
    val graph = Graph(edges)
    assert( graph.edges.count() === rawEdges.size )
  }

  test("aggregateNeighbors") {

  }

 /* test("joinVertices") {
    sc = new SparkContext("local", "test")
    val vertices = sc.parallelize(Seq(Vertex(1, "one"), Vertex(2, "two"), Vertex(3, "three")), 2)
    val edges = sc.parallelize((Seq(Edge(1, 2, "onetwo"))))
    val g: Graph[String, String] = new GraphImpl(vertices, edges)

    val tbl = sc.parallelize(Seq((1, 10), (2, 20)))
    val g1 = g.joinVertices(tbl, (v: Vertex[String], u: Int) => v.data + u)

    val v = g1.vertices.collect().sortBy(_.id)
    assert(v(0).data === "one10")
    assert(v(1).data === "two20")
    assert(v(2).data === "three")

    val e = g1.edges.collect()
    assert(e(0).data === "onetwo")
  }
  */

//  test("graph partitioner") {
//    sc = new SparkContext("local", "test")
//    val vertices = sc.parallelize(Seq(Vertex(1, "one"), Vertex(2, "two")))
//    val edges = sc.parallelize(Seq(Edge(1, 2, "onlyedge")))
//    var g = Graph(vertices, edges)
//
//    g = g.withPartitioner(4, 7)
//    assert(g.numVertexPartitions === 4)
//    assert(g.numEdgePartitions === 7)
//
//    g = g.withVertexPartitioner(5)
//    assert(g.numVertexPartitions === 5)
//
//    g = g.withEdgePartitioner(8)
//    assert(g.numEdgePartitions === 8)
//
//    g = g.mapVertices(x => x)
//    assert(g.numVertexPartitions === 5)
//    assert(g.numEdgePartitions === 8)
//
//    g = g.mapEdges(x => x)
//    assert(g.numVertexPartitions === 5)
//    assert(g.numEdgePartitions === 8)
//
//    val updates = sc.parallelize(Seq((1, " more")))
//    g = g.updateVertices(
//      updates,
//      (v, u: Option[String]) => if (u.isDefined) v.data + u.get else v.data)
//    assert(g.numVertexPartitions === 5)
//    assert(g.numEdgePartitions === 8)
//
//    g = g.reverse
//    assert(g.numVertexPartitions === 5)
//    assert(g.numEdgePartitions === 8)
//
//  }
}
