package org.apache.spark.graph

import scala.collection.JavaConversions._
import org.apache.spark.rdd.RDD

/**
 * This object implement the graphlab gather-apply-scatter api.
 */
object GraphLab {

  /**
   * Execute the GraphLab Gather-Apply-Scatter API
   *
   * @todo finish documenting GraphLab Gather-Apply-Scatter API
   *
   * @param graph The graph on which to execute the GraphLab API
   * @param gatherFunc The gather function is executed on each edge triplet
   *                   adjacent to a vertex and returns an accumulator which
   *                   is then merged using the merge function.
   * @param mergeFunc An accumulative associative operation on the result of
   *                  the gather type.
   * @param applyFunc Takes a vertex and the final result of the merge operations
   *                  on the adjacent edges and returns a new vertex value.
   * @param scatterFunc Executed after the apply function the scatter function takes
   *                    a triplet and signals whether the neighboring vertex program
   *                    must be recomputed.
   * @param numIter The maximum number of iterations to run.
   * @param gatherDirection The direction of edges to consider during the gather phase
   * @param scatterDirection The direction of edges to consider during the scatter phase
   *
   * @tparam VD The graph vertex attribute type
   * @tparam ED The graph edge attribute type
   * @tparam A The type accumulated during the gather phase
   * @return the resulting graph after the algorithm converges
   */
  def iterate[VD: ClassManifest, ED: ClassManifest, A: ClassManifest](graph: Graph[VD, ED])(
    gatherFunc: (Vid, EdgeTriplet[VD, ED]) => A,
    mergeFunc: (A, A) => A,
    applyFunc: (Vertex[VD], Option[A]) => VD,
    scatterFunc: (Vid, EdgeTriplet[VD, ED]) => Boolean,
    numIter: Int = Integer.MAX_VALUE,
    gatherDirection: EdgeDirection = EdgeDirection.In,
    scatterDirection: EdgeDirection = EdgeDirection.Out): Graph[VD, ED] = {


    // Add an active attribute to all vertices to track convergence.
    var activeGraph: Graph[(Boolean, VD), ED] = graph.mapVertices {
      case Vertex(id, data) => (true, data)
    }.cache()

    // The gather function wrapper strips the active attribute and
    // only invokes the gather function on active vertices
    def gather(vid: Vid, e: EdgeTriplet[(Boolean, VD), ED]): Option[A] = {
      if (e.vertex(vid).data._1) {
        val edge = new EdgeTriplet[VD,ED]
        edge.src = Vertex(e.src.id, e.src.data._2)
        edge.dst = Vertex(e.dst.id, e.dst.data._2)
        edge.data = e.data
        Some(gatherFunc(vid, edge))
      } else {
        None
      }
    }

    // The apply function wrapper strips the vertex of the active attribute
    // and only invokes the apply function on active vertices
    def apply(v: Vertex[((Boolean, VD), Option[A])]): (Boolean, VD) = {
      val ((active, vData), accum) = v.data
      if (active) (true, applyFunc(Vertex(v.id, vData), accum))
      else (false, vData)
    }

    // The scatter function wrapper strips the vertex of the active attribute
    // and only invokes the scatter function on active vertices
    def scatter(rawVid: Vid, e: EdgeTriplet[(Boolean, VD), ED]): Option[Boolean] = {
      val vid = e.otherVertex(rawVid).id
      if (e.vertex(vid).data._1) {
        val edge = new EdgeTriplet[VD,ED]
        edge.src = Vertex(e.src.id, e.src.data._2)
        edge.dst = Vertex(e.dst.id, e.dst.data._2)
        edge.data = e.data
//        val src = Vertex(e.src.id, e.src.data._2)
//        val dst = Vertex(e.dst.id, e.dst.data._2)
//        val edge = new EdgeTriplet[VD,ED](src, dst, e.data)
        Some(scatterFunc(vid, edge))
      } else {
        None
      }
    }

    // Used to set the active status of vertices for the next round
    def applyActive(v: Vertex[((Boolean, VD), Option[Boolean])]): (Boolean, VD) = {
      val ((prevActive, vData), newActive) = v.data
      (newActive.getOrElse(false), vData)
    }

    // Main Loop ---------------------------------------------------------------------
    var i = 0
    var numActive = activeGraph.numVertices
    while (i < numIter && numActive > 0) {

      val gathered: Graph[((Boolean, VD), Option[A]), ED] =
        activeGraph.aggregateNeighbors(gather, mergeFunc, gatherDirection)

      val applied: Graph[(Boolean, VD), ED] = gathered.mapVertices(apply).cache()

      activeGraph = applied.cache()

      // Scatter is basically a gather in the opposite direction so we reverse the edge direction
      // activeGraph: Graph[(Boolean, VD), ED]
      val scattered: Graph[((Boolean, VD), Option[Boolean]), ED] =
        activeGraph.aggregateNeighbors(scatter, _ || _, scatterDirection.reverse)
      val newActiveGraph: Graph[(Boolean, VD), ED] =
        scattered.mapVertices(applyActive)

      activeGraph = newActiveGraph.cache()

      numActive = activeGraph.vertices.map(v => if (v.data._1) 1 else 0).reduce(_ + _)
      println("Number active vertices: " + numActive)
      i += 1
    }

    // Remove the active attribute from the vertex data before returning the graph
    activeGraph.mapVertices(v => v.data._2)
  }
}









