package org.apache.spark.graphx

import org.apache.spark._
// import org.apache.spark.graphx._
import org.apache.spark.graphx.lib._
import org.apache.hadoop.io.{LongWritable, Text}
// import org.apache.hadoop.io.Text
import org.apache.hadoop.conf.Configuration
import org.apache.mahout.text.wikipedia._
import org.apache.spark.rdd.RDD
import org.apache.spark.SparkContext._
import org.apache.spark.Logging
import java.util.{HashSet => JHashSet, TreeSet => JTreeSet}

object WikiPipelineBenchmark extends Logging {

  def time[A](label: String)(fn: => A): A = {
    val startTime = System.currentTimeMillis
    logWarning("Starting %s...".format(label))
    val result = fn
    logWarning("Finished %s. TIMEX: %f".format(label, (System.currentTimeMillis - startTime) / 1000.0))
    result
  }

  def main(args: Array[String]) = {

    val host = args(0)
    val process = args(1)

    val sparkconf = new SparkConf()
      .set("spark.serializer", "org.apache.spark.serializer.KryoSerializer")
      .set("spark.kryo.registrator", "org.apache.spark.graphx.GraphKryoRegistrator")

    val sc = new SparkContext(host, "WikiPipeline", sparkconf)

   // val numParts = 64
   val parsePart = 128
   val graphPart = 64
   val start = System.currentTimeMillis
   process match {
     case "graphx" => {
       val rawData = args(2)
       val numIters = args(3).toInt
       val numPRIters = args(4).toInt
       benchmarkGraphx(sc, rawData, numIters, numPRIters, parsePart, graphPart)
     }

     case "extract" => {
       val rawData = args(2)
       val outBase = args(3)
       val (vertices, edges) = extractLinkGraph(sc, rawData, parsePart)
       val g = Graph(vertices, edges)
       val cleanG = g.subgraph(x => true, (vid, vd) => vd != null).cache
       // cleanG.triplets.count
       // cleanG.vertices.count
       val rawEdges = cleanG.edges.map(e => (e.srcId, e.dstId))
       writeGraphAsText(outBase, cleanG.vertices, rawEdges, 1)
     }

     case "analyze" => {
       val outBase = args(2)
       val iter = args(3).toInt
       val delim = args(4).toInt match {
         case 0 => ","
         case 1 => "\\s+"
       }
       pipelinePostProcessing(sc, outBase, iter, delim)
     }

     case _ => throw new IllegalArgumentException("Please provide a valid process")
   }
   logWarning(process + "\tTIMEX: " + (System.currentTimeMillis - start)/1000.0)
   sc.stop()
   System.exit(0)

  }

  def benchmarkGraphx(sc: SparkContext, rawData: String, numIters: Int, numPRIters: Int, parsePart: Int, graphPart: Int) {
    val startG = time("graph extraction") {
      val (verticesUnpart, edgesUnpart) = extractLinkGraph(sc, rawData, parsePart)
      // val vertices = verticesUnpart.coalesce(graphPart, false)
      // val edges = edgesUnpart.coalesce(graphPart, false)
      val vertices = verticesUnpart.repartition(graphPart)
      val edges = edgesUnpart.repartition(graphPart)

      val g = time("graph creation") {
        val result = Graph(vertices, edges)
        logWarning("Graph has %d vertex partitions, %d edge partitions".format(result.vertices.partitions.length, result.edges.partitions.length))
        logWarning(s"DIRTY graph has ${result.triplets.count()} EDGES, ${result.vertices.count()} VERTICES")
        result
      }
      // TODO: try reindexing
      val cleanG = g.subgraph(x => true, (vid, vd) => vd != null).partitionBy(PartitionStrategy.EdgePartition2D).cache()
      cleanG.vertices.setName("cleanG vertices")
      cleanG.edges.setName("cleanG edges")
      logWarning(s"CLEAN graph has ${cleanG.triplets.count()} EDGES, ${cleanG.vertices.count()} VERTICES")
      cleanG
    }
    val resultG = pagerankConnComponentsAlt(numIters, startG, numPRIters)
    logWarning(s"ORIGINAL graph has ${startG.triplets.count()} EDGES, ${startG.vertices.count()} VERTICES")
    logWarning(s"FINAL graph has ${resultG.triplets.count()} EDGES, ${resultG.vertices.count()} VERTICES")
  }

  def pagerankConnComponentsAlt(numRepetitions: Int, g: Graph[String, Double], numPRIters: Int): Graph[String, Double] = {
    var currentGraph = g
    logWarning("starting iterations")
    for (i <- 1 to numRepetitions) {
      currentGraph.cache()
      currentGraph.vertices.setName("currentGraph vertices %d".format(i))
      currentGraph.edges.setName("currentGraph edges %d".format(i))
      time("stage %d".format(i)) {
        // GRAPH VIEW
        time("connected components, stage %d".format(i)) {
          val ccGraph = ConnectedComponents.run(currentGraph)
          val numCCs = ccGraph.vertices.map { case (id, cc) => cc }.distinct(1).count
          logWarning(s"Number of connected components for iteration $i: $numCCs")
          ccGraph.unpersistVertices(blocking = false)
        }
        val pr =
          time("pagerank, stage %d".format(i)) {
            PageRank.run(currentGraph, numPRIters)
          }
        // TABLE VIEW
        val top20verts =
          time("top 20 pages, stage %d".format(i)) {
            val prAndTitle = currentGraph.outerJoinVertices(pr.vertices)({(id: VertexId, title: String, rank: Option[Double]) => (title, rank.getOrElse(0.0))})
            val top20 = prAndTitle.vertices.top(20)(Ordering.by((entry: (VertexId, (String, Double))) => entry._2._2))
            pr.unpersistVertices(blocking = false)
            prAndTitle.unpersistVertices(blocking = false)
            logWarning(s"Top20 for iteration $i:\n${top20.mkString("\n")}")
            top20.map(_._1).toSet
          }
        val newGraph =
          time("filter out top 20 pages, stage %d".format(i)) {
            // filter out top 20 vertices
            val filterTop20 = {(v: VertexId, d: String) =>
              !top20verts.contains(v)
            }
            val result = currentGraph.subgraph(x => true, filterTop20).cache()
            result.vertices.setName("newGraph vertices %d".format(i))
            result.edges.setName("newGraph edges %d".format(i))
            result.vertices.count
            result
          }
        currentGraph.unpersistVertices(blocking = false)
        currentGraph = newGraph
      }
    }
    currentGraph
  }

  def writeGraphAsText[V](basePath: String,
                          vertices: RDD[(VertexId, V)],
                          edges: RDD[(VertexId, VertexId)],
                          // graph: Graph[V, _],
                          iter: Int = 0) {
    val verticesToSave = vertices.map {v => s"${v._1}\t${v._2}"}
    val edgesToSave = edges.map {e => s"${e._1}\t${e._2}"}
    logWarning(s"Writing ${verticesToSave.count} VERTICES, ${edgesToSave.count} EDGES to file")
    verticesToSave.saveAsTextFile(s"${basePath}-vertices_del_$iter")
    edgesToSave.saveAsTextFile(s"${basePath}-edges_del_$iter")
  }

  def readEdgesFromText(sc: SparkContext, path: String): RDD[(VertexId, VertexId)] = {
    sc.textFile(path, 128).map { line =>
      val lineSplits = line.split("\\s+")
      (lineSplits(0).toLong, lineSplits(1).toLong)
    }
  }

  def extractLinkGraph(sc: SparkContext, rawData: String, parsePart: Int): (RDD[(VertexId, String)], RDD[Edge[Double]]) = {
    val conf = new Configuration
    conf.set("key.value.separator.in.input.line", " ")
    conf.set("xmlinput.start", "<page>")
    conf.set("xmlinput.end", "</page>")

    val xmlRDD = sc.newAPIHadoopFile(rawData, classOf[XmlInputFormat], classOf[LongWritable], classOf[Text], conf)
      .map(t => t._2.toString).coalesce(parsePart, false)
    val allArtsRDD = xmlRDD.map { raw => new WikiArticle(raw) }

    val wikiRDD =
      time("filter relevant articles") {
        val result = allArtsRDD.filter { art => art.relevant }.cache().setName("wikiRDD")
        logWarning(s"wikiRDD counted. Found ${result.count} relevant articles in ${result.partitions.size} partitions")
        result
      }
    val vertices: RDD[(VertexId, String)] = wikiRDD.map { art => (art.vertexID, art.title) }
    val edges: RDD[Edge[Double]] = wikiRDD.flatMap { art => art.edges }
    (vertices, edges)
  }

  def pipelinePostProcessing(sc: SparkContext, basePath: String, iter: Int, delim: String) {
    val pageranks = GraphLoader.loadVertices(sc, s"${basePath}-prs_del_${iter}*")
      .map {v => (v._1, v._2.toDouble) }
    val connComponents = GraphLoader.loadVertices(sc, s"${basePath}-ccs_del_${iter}*", delim)
      .map {v => (v._1, v._2.toLong) }
    val edges = readEdgesFromText(sc, s"${basePath}-edges_del_$iter")
    val artNames = GraphLoader.loadVertices(sc, s"${basePath}-vertices_del_$iter")
    val rankAndTitle = artNames.join(pageranks)
    val top20 = rankAndTitle.top(20)(Ordering.by((entry: (VertexId, (String, Double))) => entry._2._2))
    logWarning(s"Top20 for iteration $iter:\n${top20.mkString("\n")}")
    val numCCs = connComponents.map{ case (id, cc) => cc }.distinct(1).count
    logWarning(s"Number of connected components for iteration $iter: $numCCs")
    val top20verts = top20.map(_._1).toSet
    val newVertices = artNames.filter { case (v, d) => !top20verts.contains(v) }
    val newEdges = edges.filter { case (s, d) => !(top20verts.contains(s) || top20verts.contains(d)) }
    writeGraphAsText(basePath, newVertices, newEdges, iter + 1)
  }
}
