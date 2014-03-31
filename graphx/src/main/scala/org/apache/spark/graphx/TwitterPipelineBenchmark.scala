package org.apache.spark.graphx

import org.apache.spark._
// import org.apache.spark.graphx._
import org.apache.spark.graphx.lib._
// import org.apache.hadoop.io.{LongWritable, Text}
// import org.apache.hadoop.io.Text
// import org.apache.hadoop.conf.Configuration
// import org.apache.mahout.text.wikipedia._
import org.apache.spark.rdd.RDD
import org.apache.spark.SparkContext._
import org.apache.spark.Logging
import java.util.{HashSet => JHashSet, TreeSet => JTreeSet}

object TwitterPipelineBenchmark extends Logging {

  def time[A](label: String)(fn: => A): A = {
    val startTime = System.currentTimeMillis
    // logWarning("Starting %s...".format(label))
    val result = fn
    logWarning(s"${label}_TIMEX ${(System.currentTimeMillis - startTime) / 1000.0}")
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
   // val parsePart = 128
   // val start = System.currentTimeMillis
   process match {
     case "graphx" => {
       val rawData = args(2)
       val numIters = args(3).toInt
       val numPRIters = args(4).toInt
       val graphPart = args(5).toInt
       // val partStrategy = args(5).toInt
       benchmarkGraphx(sc, rawData, numIters, numPRIters, graphPart)
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
   // logWarning(process + "\tTIMEX: " + (System.currentTimeMillis - start)/1000.0)
   sc.stop()
   System.exit(0)
  }

  def benchmarkGraphx(sc: SparkContext, rawData: String, numIters: Int, numPRIters: Int, graphPart: Int) {
    val startG = time("GRAPH_CREATE") {
      val graph = GraphLoader.edgeListFile(sc, rawData, minEdgePartitions = graphPart)
        .partitionBy(PartitionStrategy.EdgePartition2D).cache()
      logWarning("Graph has %d vertex partitions, %d edge partitions".format(graph.vertices.partitions.length, graph.edges.partitions.length))
      logWarning(s"DIRTY graph has ${graph.triplets.count()} EDGES, ${graph.vertices.count()} VERTICES")
      graph
    }
    val resultG = pagerankConnComponentsAlt(numIters, startG, numPRIters)
    logWarning(s"ORIGINAL graph has ${startG.triplets.count()} EDGES, ${startG.vertices.count()} VERTICES")
    logWarning(s"FINAL graph has ${resultG.triplets.count()} EDGES, ${resultG.vertices.count()} VERTICES")
  }

  def pagerankConnComponentsAlt(numRepetitions: Int, g: Graph[Int, Int], numPRIters: Int): Graph[Int, Int] = {
    var currentGraph = g
    logWarning("starting iterations")
    for (i <- 1 to numRepetitions) {
      currentGraph.cache()
      currentGraph.vertices.setName("currentGraph vertices %d".format(i))
      currentGraph.edges.setName("currentGraph edges %d".format(i))
      time("TOTAL") {
        // GRAPH VIEW
        time("CC") {
          val ccGraph = ConnectedComponents.run(currentGraph)
          val numCCs = ccGraph.vertices.map { case (id, cc) => cc }.distinct(1).count
          logWarning(s"Number of connected components for iteration $i: $numCCs")
          ccGraph.unpersistVertices(blocking = false)
        }
        val pr =
          time("PR") {
            PageRank.run(currentGraph, numPRIters)
          }
        // TABLE VIEW
        val newGraph = time("EXTRACT") {
          val top20verts = pr.vertices.top(20)(Ordering.by((entry: (VertexId, Double)) => entry._2))
          pr.unpersistVertices(blocking = false)
          logWarning(s"Top20 for iteration $i:\n${top20verts.mkString("\n")}")
          top20verts.map(_._1).toSet
          // filter out top 20 vertices
          val filterTop20 = {(v: VertexId, d: Int) =>
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
                          // vertices: RDD[(VertexId, V)],
                          edges: RDD[(VertexId, VertexId)],
                          // graph: Graph[V, _],
                          iter: Int = 0) {
    // val verticesToSave = vertices.map {v => s"${v._1}\t${v._2}"}
    val edgesToSave = edges.map {e => s"${e._1}\t${e._2}"}
    logWarning(s"Writing ${edgesToSave.count} EDGES to file")
    // verticesToSave.saveAsTextFile(s"${basePath}-vertices_del_$iter")
    edgesToSave.saveAsTextFile(s"${basePath}-edges_del_$iter")
  }

  def readEdgesFromText(sc: SparkContext, path: String): RDD[(VertexId, VertexId)] = {
    sc.textFile(path, 128).map { line =>
      val lineSplits = line.split("\\s+")
      (lineSplits(0).toLong, lineSplits(1).toLong)
    }
  }

  def pipelinePostProcessing(sc: SparkContext, basePath: String, iter: Int, delim: String) {
    val pageranks = GraphLoader.loadVertices(sc, s"${basePath}-prs_del_${iter}*")
      .map {v => (v._1, v._2.toDouble) }
    val connComponents = GraphLoader.loadVertices(sc, s"${basePath}-ccs_del_${iter}*", delim)
      .map {v => (v._1, v._2.toLong) }
    val edges = readEdgesFromText(sc, s"${basePath}-edges_del_$iter")
    val top20 = pageranks.top(20)(Ordering.by((entry: (VertexId, Double)) => entry._2))
    logWarning(s"Top20 for iteration $iter:\n${top20.mkString("\n")}")
    val numCCs = connComponents.map{ case (id, cc) => cc }.distinct(1).count
    logWarning(s"Number of connected components for iteration $iter: $numCCs")
    val top20verts = top20.map(_._1).toSet
    val newEdges = edges.filter { case (s, d) => !(top20verts.contains(s) || top20verts.contains(d)) }
    writeGraphAsText(basePath, newEdges, iter + 1)
  }
}
