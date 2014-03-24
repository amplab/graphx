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

  def main(args: Array[String]) = {

    val host = args(0)
    val process = args(1)

    val sparkconf = new SparkConf()
      .set("spark.serializer", "org.apache.spark.serializer.KryoSerializer")
      .set("spark.kryo.registrator", "org.apache.spark.graphx.GraphKryoRegistrator")

    val sc = new SparkContext(host, "WikiPipeline", sparkconf)

   val start = System.currentTimeMillis
   process match {
     case "graphx" => {
       val rawData = args(2)
       val numIters = args(3).toInt
       benchmarkGraphx(sc, rawData, numIters)
     }

     case "extract" => {
       val rawData = args(2)
       val outBase = args(3)
       val (vertices, edges) = extractLinkGraph(sc, rawData)
       val rawEdges = edges.map(e => (e.srcId, e.dstId))
       writeGraphAsText(outBase, vertices, rawEdges, 0)
     }

     case "analyze" => {
       val outBase = args(2)
       val iter = args(3).toInt
       pipelinePostProcessing(sc, outBase, iter)
     }

     case _ => throw new IllegalArgumentException("Please provide a valid process")
   }
   logWarning(process + "\tTIMEX: " + (System.currentTimeMillis - start)/1000.0)
   sc.stop()
   System.exit(0)

  }

  def benchmarkGraphx(sc: SparkContext, rawData: String, numIters: Int) {
    val (vertices, edges) = extractLinkGraph(sc, rawData)
    logWarning("creating graph")
    val g = Graph(vertices, edges)
    val cleanG = g.subgraph(x => true, (vid, vd) => vd != null).cache
    logWarning(s"DIRTY graph has ${g.triplets.count()} EDGES, ${g.vertices.count()} VERTICES")
    logWarning(s"CLEAN graph has ${cleanG.triplets.count()} EDGES, ${cleanG.vertices.count()} VERTICES")
    val resultG = pagerankConnComponentsAlt(numIters, cleanG)
    logWarning(s"ORIGINAL graph has ${cleanG.triplets.count()} EDGES, ${cleanG.vertices.count()} VERTICES")
    logWarning(s"FINAL graph has ${resultG.triplets.count()} EDGES, ${resultG.vertices.count()} VERTICES")
  }

  def pagerankConnComponentsAlt(numRepetitions: Int, g: Graph[String, Double]): Graph[String, Double] = {
    var currentGraph = g
    logWarning("starting iterations")
    for (i <- 0 to numRepetitions) {
      currentGraph.cache
      val startTime = System.currentTimeMillis
      logWarning("starting pagerank")
      // GRAPH VIEW
      val ccGraph = ConnectedComponents.run(currentGraph).cache
      val zeroVal = new JTreeSet[VertexId]()
      val seqOp = (s: JTreeSet[VertexId], vtuple: (VertexId, VertexId)) => {
        s.add(vtuple._2)
        s
      }
      val combOp = (s1: JTreeSet[VertexId], s2: JTreeSet[VertexId]) => {
        s1.addAll(s2)
        s1
      }
      // TABLE VIEW
      val numCCs = ccGraph.vertices.aggregate(zeroVal)(seqOp, combOp).size()
      logWarning(s"Number of connected components for iteration $i: $numCCs")
      val pr = PageRank.run(currentGraph, 20).cache
      pr.vertices.count
      logWarning("Pagerank completed")
      // TABLE VIEW
      val prAndTitle = currentGraph.outerJoinVertices(pr.vertices)({(id: VertexId, title: String, rank: Option[Double]) => (title, rank.getOrElse(0.0))}).cache
      prAndTitle.vertices.count
      // logWarning("join completed.")
      val top20 = prAndTitle.vertices.top(20)(Ordering.by((entry: (VertexId, (String, Double))) => entry._2._2))
      logWarning(s"Top20 for iteration $i:\n${top20.mkString("\n")}")
      val top20verts = top20.map(_._1).toSet
      // filter out top 20 vertices
      val filterTop20 = {(v: VertexId, d: String) =>
        !top20verts.contains(v)
      }
      val newGraph = currentGraph.subgraph(x => true, filterTop20).cache
      newGraph.vertices.count
      logWarning(s"TIMEX iter $i ${(System.currentTimeMillis - startTime)/1000.0}")
      currentGraph = newGraph
    }
    currentGraph
  }

  def writeGraphAsText[V](basePath: String,
                          vertices: RDD[(VertexId, V)],
                          edges: RDD[(VertexId, VertexId)],
                          iter: Int = 0) {
    val verticesToSave = vertices.map {v => s"${v._1}\t${v._2}"}
    val edgesToSave = edges.map {e => s"${e._1}\t${e._2}"}
    verticesToSave.saveAsTextFile(s"${basePath}_vertices_$iter")
    edgesToSave.saveAsTextFile(s"${basePath}_edges_$iter")
  }

  def readEdgesFromText(sc: SparkContext, path: String): RDD[(VertexId, VertexId)] = {
    sc.textFile(path, 128).map { line =>
      val lineSplits = line.split("\\s+")
      (lineSplits(0).toInt, lineSplits(1).toInt)
    }
  }

  def extractLinkGraph(sc: SparkContext, rawData: String): (RDD[(VertexId, String)], RDD[Edge[Double]]) = {
    val conf = new Configuration
    conf.set("key.value.separator.in.input.line", " ")
    conf.set("xmlinput.start", "<page>")
    conf.set("xmlinput.end", "</page>")

    logWarning("about to load xml rdd")
    val xmlRDD = sc.newAPIHadoopFile(rawData, classOf[XmlInputFormat], classOf[LongWritable], classOf[Text], conf)
      .map(t => t._2.toString)
    // xmlRDD.count
    logWarning(s"XML RDD counted. Found ${xmlRDD.count} raw articles.")
    val repartXMLRDD = xmlRDD.repartition(128)
    logWarning(s"XML RDD repartitioned. Found ${repartXMLRDD.count} raw articles.")

    val allArtsRDD = repartXMLRDD.map { raw => new WikiArticle(raw) }.cache
    logWarning(s"Total articles: Found ${allArtsRDD.count} UNPARTITIONED articles.")

    val wikiRDD = allArtsRDD.filter { art => art.relevant }.cache //.repartition(128)
    logWarning(s"wikiRDD counted. Found ${wikiRDD.count} relevant articles in ${wikiRDD.partitions.size} partitions")
    val vertices: RDD[(VertexId, String)] = wikiRDD.map { art => (art.vertexID, art.title) }
    val edges: RDD[Edge[Double]] = wikiRDD.flatMap { art => art.edges }
    (vertices, edges)

  }

  def pipelinePostProcessing(sc: SparkContext, basePath: String, iter: Int) {
    val pageranks = GraphLoader.loadVertices(sc, s"${basePath}_${iter}_prs*")
      .map {v => (v._1, v._2.toDouble) }
    val connComponents = GraphLoader.loadVertices(sc, s"${basePath}_${iter}_ccs*")
      .map {v => (v._1, v._2.toLong) }
    val edges = readEdgesFromText(sc, s"${basePath}_edges_$iter")
    val artNames = GraphLoader.loadVertices(sc, s"${basePath}_vertices_$iter")
    val rankAndTitle = artNames.join(pageranks)
    val top20 = rankAndTitle.top(20)(Ordering.by((entry: (VertexId, (String, Double))) => entry._2._2))
    logWarning(s"Top20 for iteration $iter:\n${top20.mkString("\n")}")
    val zeroVal = new JTreeSet[VertexId]()
    val seqOp = (s: JTreeSet[VertexId], vtuple: (VertexId, VertexId)) => {
      s.add(vtuple._2)
      s
    }
    val combOp = (s1: JTreeSet[VertexId], s2: JTreeSet[VertexId]) => {
      s1.addAll(s2)
      s1
    }
    val numCCs = connComponents.aggregate(zeroVal)(seqOp, combOp).size()
    logWarning(s"Number of connected components for iteration $iter: $numCCs")
    val top20verts = top20.map(_._1).toSet
    val newVertices = artNames.filter { case (v, d) => !top20verts.contains(v) }
    val newEdges = edges.filter { case (s, d) => !(top20verts.contains(s) || top20verts.contains(d)) }
    writeGraphAsText(basePath, newVertices, newEdges, iter + 1)
  }
}
