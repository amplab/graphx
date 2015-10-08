package org.apache.spark.graphx.lib

import org.apache.spark.graphx._

import scala.reflect.ClassTag

/**
 * Created by SherlockYang.
 * calculate global clustering coefficient for a given graph
 * GCC = (3 * number_of_triangles) / number_of_connected_vertex_triplets
 * Note: directions of edges will be ignored
 */
object GlobalClusteringCoefficient {

  def run[VD: ClassTag, ED: ClassTag](graph: Graph[VD,ED]): Float = {
    // Remove redundant edges
    val g = graph.groupEdges((a, b) => a).cache()

    // count number of connected vertex triplets
    var numberOfTriplets: Int = 0
    g.collectNeighborIds(EdgeDirection.Either).collect().foreach { case (vid, nbrs) =>
      val set = new VertexSet(4)
      var i = 0
      while (i < nbrs.size) {
        // prevent self cycle
        if(nbrs(i) != vid) {
          set.add(nbrs(i))
        }
        i += 1
      }
      numberOfTriplets += set.size
    }

    // count triangles
    var numberOfTriangles: Int = 0
    val triangleCounter = graph.triangleCount()
    val verts = triangleCounter.vertices
    verts.collect().foreach { case (vid, count) =>
      numberOfTriangles += count
    }
    numberOfTriangles /= 3

    println("[GCC] #triangles: " + numberOfTriangles)
    println("[GCC] #triplets: " + numberOfTriplets)
    if (numberOfTriplets == 0)
      0
    else
      3 * numberOfTriangles.toFloat / numberOfTriplets
  }
}
