package org.apache.spark.graphx.lib

import org.apache.spark.graphx.Graph

import scala.reflect.ClassTag

/**
 * Created by SherlockYang.
 */
object SampleTriangle {
  def run[VD: ClassTag, ED: ClassTag](graph: Graph[VD,ED], p: Double): Double = {
    // Remove redundant edges
    val g = graph.groupEdges((a, b) => a).cache()

    // sample graph
    scala.util.Random.setSeed(745623)
    val subgraph = g.subgraph(epred = (edge) => scala.util.Random.nextFloat() <= p)

    // count triangles in subgraph
    val triangleCount = subgraph.triangleCount()
    val verts = triangleCount.vertices

    // approximation
    var res: Double = 0
    verts.collect().foreach { case (vid, count) =>
      res += count / (p * p * p)
    }
    res / 3
  }
}