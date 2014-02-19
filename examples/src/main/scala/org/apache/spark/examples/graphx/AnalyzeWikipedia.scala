package org.apache.spark.examples.graphx

import org.apache.spark._
import org.apache.spark.graph._
import org.apache.spark.graph.algorithms._
import org.apache.spark.rdd.NewHadoopRDD
import org.apache.hadoop.io.LongWritable
import org.apache.hadoop.io.Text
import org.apache.hadoop.conf.Configuration
import org.apache.mahout.text.wikipedia._
import org.apache.spark.rdd.RDD
import java.util.Calendar
import scala.math.Ordering.Implicits._


object AnalyzeWikipedia extends Logging {

  def main(args: Array[String]) = {




    val host = args(0)
    val fname = args(1)
    // val numparts = {
    //   if (args.length >= 3) {
    //     args(2).toInt
    //   } else {
    //     64
    //   }
    // }
    // val preformattedFname = args(2)

   val serializer = "org.apache.spark.serializer.KryoSerializer"
   System.setProperty("spark.serializer", serializer)
   System.setProperty("spark.kryo.registrator", "org.apache.spark.graph.GraphKryoRegistrator")

    val sc = new SparkContext(host, "AnalyzeWikipedia")
    // val top10 = sc.parallelize(1 to 1000, 10).map(x => (x.toString, x)).top(10)(Ordering.by(_._2))


    // val conf = new Configuration
    // conf.set("key.value.separator.in.input.line", " ");
    // conf.set("xmlinput.start", "<page>");
    // conf.set("xmlinput.end", "</page>");

    // val xmlRDD = sc.newAPIHadoopFile(fname, classOf[XmlInputFormat], classOf[LongWritable], classOf[Text], conf)
    //   .map(stringify)

    // println("XML pages: " + xmlRDD.count)
    //   // .repartition(numparts)

    // val wikiRDD = xmlRDD.map { raw => new WikiArticle(raw) }
    //   .filter { art => art.relevant }

    // println("Relevant pages: " + wikiRDD.count)

    // val vertices: RDD[(Vid, String)] = wikiRDD.map { art => (art.vertexID, art.title) }
    // val justVids = wikiRDD.map { art => art.vertexID }
    // // println("taking top vids")
    // // val topvids = justVids.top(10)
    // // sc.stop()
    // // System.exit(0)

    // // val edges: RDD[Edge[Double]] = wikiRDD.flatMap { art => art.edges }
    // val edges: RDD[Edge[Double]] = wikiRDD.flatMap { art => art.edges }
    // println("Edges: " + edges.count)
    // println("Creating graph: " + Calendar.getInstance().getTime())

    // val g = Graph(vertices, edges)
    // val g = Graph.fromEdges(edges, 1)
    // val g = Graph(edges, 1)
    val g = GraphLoader.edgeListAndVertexListFiles(sc, fname + "_edges", fname + "_vertices",
      minEdgePartitions = 128).cache()
    println("Triplets: " + g.triplets.count)

    println("starting pagerank " + Calendar.getInstance().getTime())
    val startTime = System.currentTimeMillis
    val pr = PageRank.run(g, 20)

    println("PR numvertices: " + pr.vertices.count + "\tOriginal numVertices " + g.vertices.count)
    println("Pagerank runtime:    " + ((System.currentTimeMillis - startTime)/1000.0) + " seconds")
    val prAndTitle = g.outerJoinVertices(pr.vertices)({(id: Vid, title: String, rank: Option[Double]) => (title, rank.getOrElse(0.0))})
    println("finished join.")

    val topArticles = prAndTitle.vertices.top(30)(Ordering.by((entry: (Vid, (String, Double))) => entry._2._2))
    println("Top articles:\n" + topArticles.deep.mkString("\n"))
    // for(v <- topArticles) {
    //   println(v)
    // }
    val article_name = "JohnsHopkinsUniversity"
    //
    //Find relevant vertices
    g.mapTriplets(e => {
      if ((e.srcAttr contains article_name) || (e.dstAttr contains article_name)) { 1.0 }
      else { e.attr }
    })
    val coarsenedGraph = g.contractEdges({ e => e.attr == 1.0 }, {et => et.srcAttr + " " + et.dstAttr },
    { (v1: String , v2: String) => v1 + "\n" + v2 })
    
    // filter only vertices whose title contains JHU
    val relevant = coarsenedGraph.vertices.filter( {case (vid: Vid, data: String) => data contains article_name}).collect
    println("Articles matching " + article_name)
    println(relevant.deep.mkString("New Article\n"))

    sc.stop()
  }


  def stringify(tup: (org.apache.hadoop.io.LongWritable, org.apache.hadoop.io.Text)): String = {
    tup._2.toString
  }



}
