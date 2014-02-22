package org.apache.spark.examples.graphx

import org.apache.spark._
import org.apache.spark.graphx._
import org.apache.spark.graphx.lib._
import org.apache.hadoop.io.LongWritable
import org.apache.hadoop.io.Text
import org.apache.hadoop.conf.Configuration
import org.apache.mahout.text.wikipedia._
import org.apache.spark.rdd.RDD
import org.apache.spark.SparkContext._
import org.apache.spark.Logging
import scala.collection.mutable


object PrePostProcessWikipedia extends Logging {

  def main(args: Array[String]) = {

    val host = args(0)
    val process = args(1)

    val serializer = "org.apache.spark.serializer.KryoSerializer"
    System.setProperty("spark.serializer", serializer)
    System.setProperty("spark.kryo.registrator", "org.apache.spark.graph.GraphKryoRegistrator")

    val sc = new SparkContext(host, "ETL")

   val start = System.currentTimeMillis
   process match {
     case "pre" => {
       val fname = args(2)
       val outbase = args(3)
       preProcess(sc, fname, outbase)
     }
     case "post" => {
       val rankPath = args(2)
       val attributePath = args(3)
       val result = postProcess(sc, rankPath, attributePath)
       logWarning(result)
     }
     case "graphx" => {
       val rawData = args(2)
       val result = graphx(sc, rawData)
//       logWarning(result)
     }
     case "prep" => {
       val rawData = args(2)
       val outBase = args(3)
       prep(sc, rawData, outBase)
     }

     case _ => throw new IllegalArgumentException("Please proVertexIde a valid process")
   }
   logWarning(process + "\tTIMEX: " + (System.currentTimeMillis - start)/1000.0)
   sc.stop()
   System.exit(0)

  }



  def prep(sc: SparkContext, rawData: String, outBase: String) {

    val conf = new Configuration
    conf.set("key.value.separator.in.input.line", " ");
    conf.set("xmlinput.start", "<page>");
    conf.set("xmlinput.end", "</page>");

    val vertPath = outBase + "_vertices"
    val rankPath = outBase + "_ranks"
    val xmlRDD = sc.newAPIHadoopFile(rawData, classOf[XmlInputFormat], classOf[LongWritable], classOf[Text], conf)
      .map(stringify)
    val wikiRDD = xmlRDD.map { raw => new WikiArticle(raw) }
      .filter { art => art.relevant }.repartition(128)
    val vertices: RDD[(VertexId, String)] = wikiRDD.map { art => (art.vertexID, art.title) }
    val verticesToSave = vertices.map {v => v._1 + "\t"+ v._2}
    verticesToSave.saveAsTextFile(vertPath)
    val edges: RDD[Edge[Double]] = wikiRDD.flatMap { art => art.edges }
    val g = Graph(vertices, edges) //TODO what to do about partitionStrategy???
    val pr = PageRank.run(g, 20)
    val prToSave = pr.vertices.map {v => v._1 + "\t"+ v._2}
    prToSave.saveAsTextFile(rankPath)
  }

  def graphx(sc: SparkContext, rawData: String) {

    val conf = new Configuration
    conf.set("key.value.separator.in.input.line", " ")
    conf.set("xmlinput.start", "<page>")
    conf.set("xmlinput.end", "</page>")

    val xmlRDD = sc.newAPIHadoopFile(rawData, classOf[XmlInputFormat], classOf[LongWritable], classOf[Text], conf)
      .map(stringify)
    val wikiRDD = xmlRDD.map { raw => new WikiArticle(raw) }
      .filter { art => art.relevant }.repartition(128)
    val vertices: RDD[(VertexId, String)] = wikiRDD.map { art => (art.vertexID, art.title) }
    val edges: RDD[Edge[Double]] = wikiRDD.flatMap { art => art.edges }
    val g = Graph(vertices, edges)
    val resultG = pagerankConnComponentsAlt(4, g)
    logWarning(s"Final graph has ${resultG.triplets.count()} EDGES, ${resultG.vertices.count()} VERTICES")
//     val pr = PageRank.run(g, 20)
//     val prAndTitle = g
//       .outerJoinVertices(pr)({(id: VertexId, title: String, rank: Option[Double]) => (title, rank.getOrElse(0.0))})
//     val top20 = prAndTitle.vertices.top(20)(Ordering.by((entry: (VertexId, (String, Double))) => entry._2._2))
//     top20.mkString("\n")

  }

  def pagerankConnComponentsAlt(numRepetitions: Int, g: Graph[String, Double]): Graph[String, Double] = {
    var currentGraph = g
    for (i <- 0 to numRepetitions) {
      val pr = PageRank.run(currentGraph, 20)
      val prAndTitle = currentGraph.outerJoinVertices(pr.vertices)({(id: VertexId, title: String, rank: Option[Double]) => (title, rank.getOrElse(0.0))})
      val top20 = prAndTitle.vertices.top(20)(Ordering.by((entry: (VertexId, (String, Double))) => entry._2._2))
      logWarning(s"Top20 for iteration $i:\n${top20.mkString("\n")}")
      val top20verts = top20.map(_._1).toSet
      // filter out top 20 vertices
      val filterTop20 = {(v: VertexId, d: String) =>
        !top20verts.contains(v)
      }
      val newGraph = currentGraph.subgraph(x => true, filterTop20)
      val ccGraph = ConnectedComponents.run(newGraph)
      val zeroVal = new mutable.HashSet[VertexId]()
      val seqOp = (s: mutable.HashSet[VertexId], vtuple: (VertexId, VertexId)) => {
        s.add(vtuple._2)
        s
      }
      val combOp = (s1: mutable.HashSet[VertexId], s2: mutable.HashSet[VertexId]) => { s1 union s2}
      val numCCs = ccGraph.vertices.aggregate(zeroVal)(seqOp, combOp)
      //(new mutable.HashSet[Int]())((s: mutable.HashSet[Int], vtuple: (VertexId, Int)) => { s.add(vtuple._2); s },(s1: mutable.HashSet[Int], s2: mutable.HashSet[Int]) => { s1 union s2})

      //(((set, vtuple) => set.add(vtuple._2)), ((set1, set2) => set1 union set2)).size
      logWarning(s"Number of connected components for iteration $i: $numCCs")
      // TODO will this result in too much memory overhead???
      currentGraph = newGraph
    }
    currentGraph
  }

  // parse wikipedia xml dump and
  def preProcess(sc: SparkContext, rawData: String, outBase: String) = {

    val conf = new Configuration
    conf.set("key.value.separator.in.input.line", " ");
    conf.set("xmlinput.start", "<page>");
    conf.set("xmlinput.end", "</page>");

    val xmlRDD = sc.newAPIHadoopFile(rawData, classOf[XmlInputFormat], classOf[LongWritable], classOf[Text], conf)
      .map(stringify)
    val wikiRDD = xmlRDD.map { raw => new WikiArticle(raw) }
      .filter { art => art.relevant }.repartition(128)
    val vertices: RDD[(VertexId, String)] = wikiRDD.map { art => (art.vertexID, art.title) }
    val verticesToSave = vertices.map {v => v._1 + "\t"+ v._2}
    verticesToSave.saveAsTextFile(outBase + "_vertices")
    val edges: RDD[Edge[Double]] = wikiRDD.flatMap { art => art.edges }
    val edgesToSave = edges.map(e => e.srcId + "\t" + e.dstId)
    edgesToSave.saveAsTextFile(outBase + "_edges")

  }


  def postProcess(sc: SparkContext, rankPath: String, attrPath: String): String = {
    val ranks = GraphLoader.loadVertices(sc, rankPath).map {v => (v._1, v._2.toDouble)}
    val attrs = GraphLoader.loadVertices(sc, attrPath)

    // slightly cheating, but not really
    val ranksAndAttrs = ranks.join(attrs)
    val top20 = ranksAndAttrs.top(20)(Ordering.by((entry: (VertexId, (Double, String))) => entry._2._1))
    top20.mkString("\n")
  }

  def stringify(tup: (org.apache.hadoop.io.LongWritable, org.apache.hadoop.io.Text)): String = {
    tup._2.toString
  }



}
