package org.apache.spark.graph

import java.util.{Arrays => JArrays}

import org.apache.spark.Logging
import org.apache.spark.SparkContext
import org.apache.spark.SparkContext._
import org.apache.spark.graph.impl.{EdgePartition, GraphImpl}
import org.apache.spark.rdd.RDD
import org.apache.spark.util.collection.PrimitiveVector


object GraphLoader extends Logging {

  def textFile(sc: SparkContext, path: String, minEdgePartitions: Int): GraphImpl[Int, Double] = {
    val startTime = System.currentTimeMillis

    // Parse the edge data table
    val edges = sc.textFile(path, minEdgePartitions).mapPartitionsWithIndex { (index, iter) =>
      val srcIds = new PrimitiveVector[Long]
      val dstIds = new PrimitiveVector[Long]
      iter.foreach { line =>
        if (!line.isEmpty && line(0) != '#') {
          val lineArray = line.split("\\s+")
          if (lineArray.length < 2) {
            logWarning("Invalid line: " + line)
          }
          srcIds += lineArray(0).toLong
          dstIds += lineArray(1).toLong
        }
      }
      val srcIdArray = srcIds.trim().array
      val dstIdArray = dstIds.trim().array
      val data = new Array[Double](srcIdArray.length)
      JArrays.fill(data, 1.0F)

      Iterator((index, new EdgePartition[Double](srcIdArray, dstIdArray, data)))
    }.cache()
    edges.count()

    println("It took %d ms to load the edges".format(System.currentTimeMillis - startTime))

    //val graph = fromEdges(edges)
    //graph
    null
  }

  /**
   * Load an edge list from file initializing the Graph
   *
   * @tparam ED the type of the edge data of the resulting Graph
   *
   * @param sc the SparkContext used to construct RDDs
   * @param path the path to the text file containing the edge list
   * @param edgeParser a function that takes an array of strings and
   * returns an ED object
   * @param minEdgePartitions the number of partitions for the
   * the Edge RDD
   *
   * @todo remove minVertexPartitions arg
   */
  def textFile[ED: ClassManifest](
      sc: SparkContext,
      path: String,
      edgeParser: Array[String] => ED,
      minEdgePartitions: Int = 1,
      minVertexPartitions: Int = 1,
      partitionStrategy: PartitionStrategy = RandomVertexCut()): GraphImpl[Int, ED] = {

    return textFile(sc, path, minEdgePartitions).asInstanceOf[GraphImpl[Int, ED]]

    val startTime = System.currentTimeMillis

    // Parse the edge data table
    val edges = sc.textFile(path, minEdgePartitions).flatMap { line =>
      if (!line.isEmpty && line(0) != '#') {
        val lineArray = line.split("\\s+")
        if (lineArray.length < 2) {
          println("Invalid line: " + line)
          assert(false)
        }
        val source = lineArray(0)
        val target = lineArray(1)
        val tail = lineArray.drop(2)
        val edata = edgeParser(tail)
        Array(Edge(source.trim.toInt, target.trim.toInt, edata))
      } else {
        Array.empty[Edge[ED]]
      }
    }.cache()
    edges.count()

    println("It took %d ms to load the edges".format(System.currentTimeMillis - startTime))

    val graph = fromEdges(edges, partitionStrategy)
    graph
  }

  private def fromEdges[ED: ClassManifest](
    edges: RDD[Edge[ED]],
    partitionStrategy: PartitionStrategy): GraphImpl[Int, ED] = {
    val vertices = edges.flatMap { edge => List((edge.srcId, 1), (edge.dstId, 1)) }
      .reduceByKey(_ + _)
    GraphImpl(vertices, edges, 0, (a: Int, b: Int) => a, partitionStrategy)
  }
}
