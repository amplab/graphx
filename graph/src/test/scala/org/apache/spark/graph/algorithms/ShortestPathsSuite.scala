package org.apache.spark.graph.algorithms

import org.scalatest.FunSuite

import org.apache.spark.SparkContext
import org.apache.spark.SparkContext._
import org.apache.spark.graph._
import org.apache.spark.graph.util.GraphGenerators
import org.apache.spark.rdd._


class ShortestPathsSuite extends FunSuite with LocalSparkContext {

  test("Shortest Path Computations") {
    withSpark { sc =>
      val shortestPaths = Set((1,Map(1 -> 0, 4 -> 2)), (2,Map(1 -> 1, 4 -> 2)), (3,Map(1 -> 2, 4 -> 1)),
                              (4,Map(1 -> 2, 4 -> 0)), (5,Map(1 -> 1, 4 -> 1)), (6,Map(1 -> 3, 4 -> 1)))
      val edgeSeq = Seq((1, 2), (1, 5), (2, 3), (2, 5), (3, 4), (4, 5), (4, 6)).flatMap{ case e => Seq(e, e.swap) }
      val edges = sc.parallelize(edgeSeq).map { case (v1, v2) => (v1.toLong, v2.toLong) }
      val graph = Graph.fromEdgeTuples(edges, 1)
      val landmarks = Seq(1, 4).map(_.toLong)
      val results = ShortestPaths.run(graph, landmarks).vertices.collect.map { case (v, spMap) => (v, spMap.mapValues(_.get)) }
      assert(results.toSet === shortestPaths)
    }
  }

}
