package org.apache.spark.graph

import org.apache.spark.rdd.RDD
import org.apache.spark.util.ClosureCleaner
import org.apache.spark.Logging
import org.apache.spark.storage.StorageLevel

/**
 * The Graph abstractly represents a graph with arbitrary objects
 * associated with vertices and edges.  The graph provides basic
 * operations to access and manipulate the data associated with
 * vertices and edges as well as the underlying structure.  Like Spark
 * RDDs, the graph is a functional data-structure in which mutating
 * operations return new graphs.
 *
 * @see GraphOps for additional graph member functions.
 *
 * @note The majority of the graph operations are implemented in
 * `GraphOps`.  All the convenience operations are defined in the
 * `GraphOps` class which may be shared across multiple graph
 * implementations.
 *
 * @tparam VD the vertex attribute type
 * @tparam ED the edge attribute type
 */
abstract class Graph[VD: ClassManifest, ED: ClassManifest] extends Logging {

  /**
   * Get the vertices and their data.
   *
   * @note vertex ids are unique.
   * @return An RDD containing the vertices in this graph
   *
   * @see Vertex for the vertex type.
   *
   */
  val vertices: VertexSetRDD[VD]

  /**
   * Get the Edges and their data as an RDD.  The entries in the RDD
   * contain just the source id and target id along with the edge
   * data.
   *
   * @return An RDD containing the edges in this graph
   *
   * @see Edge for the edge type.
   * @see edgesWithVertices to get an RDD which contains all the edges
   * along with their vertex data.
   *
   */
  val edges: RDD[Edge[ED]]

  /**
   * Get the edges with the vertex data associated with the adjacent
   * pair of vertices.
   *
   * @return An RDD containing edge triplets.
   *
   * @example This operation might be used to evaluate a graph
   * coloring where we would like to check that both vertices are a
   * different color.
   * {{{
   * type Color = Int
   * val graph: Graph[Color, Int] = Graph.textFile("hdfs://file.tsv")
   * val numInvalid = graph.edgesWithVertices()
   *   .map(e => if(e.src.data == e.dst.data) 1 else 0).sum
   * }}}
   *
   * @see edges() If only the edge data and adjacent vertex ids are
   * required.
   *
   */
  val triplets: RDD[EdgeTriplet[VD, ED]]



  def persist(newLevel: StorageLevel): Graph[VD, ED]


  /**
   * Return a graph that is cached when first created. This is used to
   * pin a graph in memory enabling multiple queries to reuse the same
   * construction process.
   *
   * @see RDD.cache() for a more detailed explanation of caching.
   */
  def cache(): Graph[VD, ED]


  /**
   * Compute statistics describing the graph representation.
   */
  def statistics: Map[String, Any]



  /**
   * Construct a new graph where each vertex value has been
   * transformed by the map function.
   *
   * @note This graph is not changed and that the new graph has the
   * same structure.  As a consequence the underlying index structures
   * can be reused.
   *
   * @param map the function from a vertex object to a new vertex value.
   *
   * @tparam VD2 the new vertex data type
   *
   * @example We might use this operation to change the vertex values
   * from one type to another to initialize an algorithm.
   * {{{
   * val rawGraph: Graph[(), ()] = Graph.textFile("hdfs://file")
   * val root = 42
   * var bfsGraph = rawGraph
   *   .mapVertices[Int]((vid, data) => if(vid == root) 0 else Math.MaxValue)
   * }}}
   *
   */
  def mapVertices[VD2: ClassManifest](map: (Vid, VD) => VD2): Graph[VD2, ED]

  /**
   * Construct a new graph where each the value of each edge is
   * transformed by the map operation.  This function is not passed
   * the vertex value for the vertices adjacent to the edge.  If
   * vertex values are desired use the mapTriplets function.
   *
   * @note This graph is not changed and that the new graph has the
   * same structure.  As a consequence the underlying index structures
   * can be reused.
   *
   * @param map the function from an edge object to a new edge value.
   *
   * @tparam ED2 the new edge data type
   *
   * @example This function might be used to initialize edge
   * attributes.
   *
   */
  def mapEdges[ED2: ClassManifest](map: Edge[ED] => ED2): Graph[VD, ED2]

  /**
   * Construct a new graph where each the value of each edge is
   * transformed by the map operation.  This function passes vertex
   * values for the adjacent vertices to the map function.  If
   * adjacent vertex values are not required, consider using the
   * mapEdges function instead.
   *
   * @note This graph is not changed and that the new graph has the
   * same structure.  As a consequence the underlying index structures
   * can be reused.
   *
   * @param map the function from an edge object to a new edge value.
   *
   * @tparam ED2 the new edge data type
   *
   * @example This function might be used to initialize edge
   * attributes based on the attributes associated with each vertex.
   * {{{
   * val rawGraph: Graph[Int, Int] = someLoadFunction()
   * val graph = rawGraph.mapTriplets[Int]( edge =>
   *   edge.src.data - edge.dst.data)
   * }}}
   *
   */
  def mapTriplets[ED2: ClassManifest](
    map: EdgeTriplet[VD, ED] => ED2): Graph[VD, ED2]


  /**
   * Construct a new graph with all the edges reversed.  If this graph
   * contains an edge from a to b then the returned graph contains an
   * edge from b to a.
   *
   */
  def reverse: Graph[VD, ED]


  /**
   * This function takes a vertex and edge predicate and constructs
   * the subgraph that consists of vertices and edges that satisfy the
   * predict.  The resulting graph contains the vertices and edges
   * that satisfy:
   *
   * {{{
   * V' = {v : for all v in V where vpred(v)}
   * E' = {(u,v): for all (u,v) in E where epred((u,v)) && vpred(u) && vpred(v)}
   * }}}
   *
   * @param epred the edge predicate which takes a triplet and
   * evaluates to true if the edge is to remain in the subgraph.  Note
   * that only edges in which both vertices satisfy the vertex
   * predicate are considered.
   *
   * @param vpred the vertex predicate which takes a vertex object and
   * evaluates to true if the vertex is to be included in the subgraph
   *
   * @return the subgraph containing only the vertices and edges that
   * satisfy the predicates.
   */
  def subgraph(epred: EdgeTriplet[VD,ED] => Boolean = (x => true),
    vpred: (Vid, VD) => Boolean = ((v,d) => true) ): Graph[VD, ED]



  /**
   * groupEdgeTriplets is used to merge multiple edges that have the
   * same source and destination vertex into a single edge. The user
   * supplied function is applied to each directed pair of vertices
   * (u, v) and has access to all EdgeTriplets
   *
   * {e: for all e in E where e.src = u and e.dst = v}
   *
   * This function is identical to
   * [[org.apache.spark.graph.Graph.groupEdges]] except that this
   * function provides the user-supplied function with an iterator
   * over EdgeTriplets, which contain the vertex data, whereas
   * groupEdges provides the user-supplied function with an iterator
   * over Edges, which only contain the vertex IDs.
   *
   * @tparam ED2 the type of the resulting edge data after grouping
   *
   * @param f the user supplied function to merge multiple EdgeTriplets
   * into a single ED2 object
   *
   * @return Graph[VD,ED2] The resulting graph with a single Edge for each
   * source, dest vertex pair.
   *
   */
  def groupEdgeTriplets[ED2: ClassManifest](f: Iterator[EdgeTriplet[VD,ED]] => ED2 ): Graph[VD,ED2]


  /**
   * This function merges multiple edges between two vertices into a
   * single Edge. See
   * [[org.apache.spark.graph.Graph.groupEdgeTriplets]] for more
   * detail.
   *
   * @tparam ED2 the type of the resulting edge data after grouping.
   *
   * @param f the user supplied function to merge multiple Edges
   * into a single ED2 object.
   *
   * @return Graph[VD,ED2] The resulting graph with a single Edge for
   * each source, dest vertex pair.
   */
  def groupEdges[ED2: ClassManifest](f: Iterator[Edge[ED]] => ED2 ): Graph[VD,ED2]


  /**
   * The mapReduceTriplets function is used to compute statistics
   * about the neighboring edges and vertices of each vertex.  The
   * user supplied `mapFunc` function is invoked on each edge of the
   * graph generating 0 or more "messages" to be "sent" to either
   * vertex in the edge.  The `reduceFunc` is then used to combine the
   * output of the map phase destined to each vertex.
   *
   * @tparam A the type of "message" to be sent to each vertex
   *
   * @param mapFunc the user defined map function which returns 0 or
   * more messages to neighboring vertices.
   *
   * @param reduceFunc the user defined reduce function which should
   * be commutative and assosciative and is used to combine the output
   * of the map phase.
   *
   * @example We can use this function to compute the inDegree of each
   * vertex
   * {{{
   * val rawGraph: Graph[(),()] = Graph.textFile("twittergraph")
   * val inDeg: RDD[(Vid, Int)] =
   *   mapReduceTriplets[Int](et => Array((et.dst.id, 1)), _ + _)
   * }}}
   *
   * @note By expressing computation at the edge level we achieve
   * maximum parallelism.  This is one of the core functions in the
   * Graph API in that enables neighborhood level computation. For
   * example this function can be used to count neighbors satisfying a
   * predicate or implement PageRank.
   *
   */
  def mapReduceTriplets[A: ClassManifest](
      mapFunc: EdgeTriplet[VD, ED] => Array[(Vid, A)],
      reduceFunc: (A, A) => A)
    : VertexSetRDD[A]


  /**
   * Join the vertices with an RDD and then apply a function from the
   * the vertex and RDD entry to a new vertex value and type.  The
   * input table should contain at most one entry for each vertex.  If
   * no entry is provided the map function is invoked passing none.
   *
   * @tparam U the type of entry in the table of updates
   * @tparam VD2 the new vertex value type
   *
   * @param table the table to join with the vertices in the graph.
   * The table should contain at most one entry for each vertex.
   *
   * @param mapFunc the function used to compute the new vertex
   * values.  The map function is invoked for all vertices, even those
   * that do not have a corresponding entry in the table.
   *
   * @example This function is used to update the vertices with new
   * values based on external data.  For example we could add the out
   * degree to each vertex record
   *
   * {{{
   * val rawGraph: Graph[(),()] = Graph.textFile("webgraph")
   * val outDeg: RDD[(Vid, Int)] = rawGraph.outDegrees()
   * val graph = rawGraph.outerJoinVertices(outDeg) {
   *   (vid, data, optDeg) => optDeg.getOrElse(0)
   * }
   * }}}
   *
   */
  def outerJoinVertices[U: ClassManifest, VD2: ClassManifest](table: RDD[(Vid, U)])
      (mapFunc: (Vid, VD, Option[U]) => VD2)
    : Graph[VD2, ED]


  // Save a copy of the GraphOps object so there is always one unique GraphOps object
  // for a given Graph object, and thus the lazy vals in GraphOps would work as intended.
  val ops = new GraphOps(this)
} // end of Graph




/**
 * The Graph object contains a collection of routines used to
 * construct graphs from RDDs.
 *
 */
object Graph {

  import org.apache.spark.graph.impl._
  import org.apache.spark.SparkContext._

  /**
   * Construct a graph from a collection of edges encoded as vertex id
   * pairs.  Duplicate directed edges are merged to a single edge with
   * weight equal to the number of duplicate edges.  The returned
   * vertex attribute is the number of edges adjacent to that vertex
   * (i.e., the undirected degree).
   *
   * @param rawEdges the RDD containing the set of edges in the graph
   *
   * @return a graph with edge attributes containing the count of
   * duplicate edges and vertex attributes containing the total degree
   * of each vertex.
   */
  def apply(rawEdges: RDD[(Vid, Vid)]): Graph[Int, Int] = { Graph(rawEdges, true) }


  /**
   * Construct a graph from a collection of edges encoded as vertex id
   * pairs.
   *
   * @param rawEdges a collection of edges in (src,dst) form.
   * @param uniqueEdges if multiple identical edges are found they are
   * combined and the edge attribute is set to the sum.  Otherwise
   * duplicate edges are treated as separate.
   *
   * @return a graph with edge attributes containing either the count
   * of duplicate edges or 1 (if `uniqueEdges=false`) and vertex
   * attributes containing the total degree of each vertex.
   *
   */
  def apply(rawEdges: RDD[(Vid, Vid)], uniqueEdges: Boolean): Graph[Int, Int] = {
    // Reduce to unique edges.
    val edges: RDD[Edge[Int]] =
      if (uniqueEdges) {
        rawEdges.map((_, 1)).reduceByKey(_ + _).map { case ((s, t), cnt) => Edge(s, t, cnt) }
      } else {
        rawEdges.map { case (s, t) => Edge(s, t, 1) }
      }
    // Determine unique vertices
    /** @todo Should this reduceByKey operation be indexed? */
    val vertices: RDD[(Vid, Int)] =
      edges.flatMap{ case Edge(s, t, cnt) => Array((s, 1), (t, 1)) }.reduceByKey(_ + _)

    // Return graph
    GraphImpl(vertices, edges, 0)
  }


  /**
   * Construct a graph from a collection attributed vertices and
   * edges.
   *
   * @note Duplicate vertices are removed arbitrarily and missing
   * vertices (vertices in the edge collection that are not in the
   * vertex collection) are replaced by null vertex attributes.
   *
   * @tparam VD the vertex attribute type
   * @tparam ED the edge attribute type
   * @param vertices the "set" of vertices and their attributes
   * @param edges the collection of edges in the graph
   *
   */
  def apply[VD: ClassManifest, ED: ClassManifest](
      vertices: RDD[(Vid,VD)],
      edges: RDD[Edge[ED]]): Graph[VD, ED] = {
    val defaultAttr: VD = null.asInstanceOf[VD]
    Graph(vertices, edges, defaultAttr, (a:VD,b:VD) => a)
  }


  /**
   * Construct a graph from a collection attributed vertices and
   * edges.  Duplicate vertices are combined using the `mergeFunc` and
   * vertices found in the edge collection but not in the input
   * vertices are the default attribute `defautVertexAttr`.
   *
   * @tparam VD the vertex attribute type
   * @tparam ED the edge attribute type
   * @param vertices the "set" of vertices and their attributes
   * @param edges the collection of edges in the graph
   * @param defaultVertexAttr the default vertex attribute to use for
   * vertices that are mentioned in `edges` but not in `vertices
   * @param mergeFunc the function used to merge duplicate vertices
   * in the `vertices` collection.
   *
   */
  def apply[VD: ClassManifest, ED: ClassManifest](
      vertices: RDD[(Vid,VD)],
      edges: RDD[Edge[ED]],
      defaultVertexAttr: VD,
      mergeFunc: (VD, VD) => VD): Graph[VD, ED] = {
    GraphImpl(vertices, edges, defaultVertexAttr, mergeFunc)
  }

  /**
   * The implicit graphToGraphOPs function extracts the GraphOps
   * member from a graph.
   *
   * To improve modularity the Graph type only contains a small set of
   * basic operations.  All the convenience operations are defined in
   * the GraphOps class which may be shared across multiple graph
   * implementations.
   */
  implicit def graphToGraphOps[VD: ClassManifest, ED: ClassManifest](g: Graph[VD, ED]) = g.ops
} // end of Graph object
