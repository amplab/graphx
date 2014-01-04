package org.apache.spark.graph.algorithms

import org.apache.spark.graph._
import org.apache.spark.rdd.RDD
import org.apache.spark._
import org.apache.spark.broadcast._
import org.apache.spark.util.BoundedPriorityQueue

object LDA {
  type DocId = Vid
  type WordId = Vid
  type TopicId = Int
  type Count = Int

  type Factor = Array[Count]

  class Posterior (docs: VertexRDD[Factor], words: VertexRDD[Factor])

  def addEq(a: Factor, b: Factor): Factor = {
    assert(a.size == b.size)
    var i = 0
    while (i < a.size) {
      a(i) += b(i)
      i += 1
    }
    a
  }

  def addEq(a: Factor, t: TopicId): Factor = { a(t) += 1; a }

  def makeFactor(nTopics: Int, topic: TopicId): Factor = {
    val f = new Factor(nTopics)
    f(topic) += 1
    f
  }
} // end of LDA singleton





class LDA(@transient val tokens: RDD[(LDA.WordId, LDA.DocId)],
          val nTopics: Int = 100,
          val alpha: Double = 0.1,
          val beta: Double = 0.1) {
  import LDA._

  private val sc = tokens.sparkContext

  /**
   * The bipartite terms by document graph.
   */
  var graph: Graph[Factor, TopicId] = {
    // To setup a bipartite graph it is necessary to ensure that the document and
    // word ids are in a different namespace
    val renumbered = tokens.map { case (wordId, docId) =>
      assert(wordId >= 0)
      assert(docId >= 0)
      val newDocId: DocId = -(docId + 1L)
      (wordId, newDocId)
    }
    val nT = nTopics
    // Sample the tokens
    val gTmp = Graph.fromEdgeTuples(renumbered, false).mapEdges { (pid, iter) =>
        val gen = new java.util.Random(pid)
        iter.map(e => gen.nextInt(nT))
      }
    // Compute the topic histograms (factors) for each word and document
    val newCounts = gTmp.mapReduceTriplets[Factor](
      e => Iterator((e.srcId, makeFactor(nT, e.attr)), (e.dstId, makeFactor(nT, e.attr))),
      (a, b) => addEq(a,b) )
    // Update the graph with the factors
    gTmp.outerJoinVertices(newCounts) { (_, _, newFactorOpt) => newFactorOpt.get }.cache
    // Trigger computation of the topic counts
  }

  /**
   * The number of unique words in the corpus
   */
  val nWords = graph.vertices.filter{ case (vid, c) => vid >= 0 }.count()

  /**
   * The number of documents in the corpus
   */
  val nDocs = graph.vertices.filter{ case (vid, c) => vid < 0 }.count()

  /**
   * The number of tokens
   */
  val nTokens = graph.edges.count()

  /**
   * The total counts for each topic
   */
  var totalHist = graph.edges.map(e => e.attr)
    .aggregate(new Factor(nTopics))(LDA.addEq(_, _), LDA.addEq(_, _))
  assert(totalHist.sum == nTokens)

  /**
   * The internal iteration tracks the number of times the random number
   * generator was created.  In constructing the graph the generated is created
   * once and then once for each iteration
   */
  private var internalIteration = 1


  /**
   * Run the gibbs sampler
   * @param nIter
   * @return
   */
  def iterate(nIter: Int = 1) {
    // Run the sampling
    for (i <- 0 until nIter) {
      println("Starting iteration: " + i.toString)
      // Broadcast the topic histogram
      val totalHistbcast = sc.broadcast(totalHist)
      // Shadowing because scala's closure capture is an abomination
      val a = alpha
      val b = beta
      val nt = nTopics
      val nw = nWords
      // Define the function to sample a single token
      // I had originally used def sampleToken but this leads to closure capture of the LDA class (an abomination).
      val sampleToken = (gen: java.util.Random, triplet: EdgeTriplet[Factor, TopicId]) => {
        val wHist: Array[Count] = triplet.srcAttr
        val dHist: Array[Count] = triplet.dstAttr
        val totalHist: Array[Count] = totalHistbcast.value
        val oldTopic = triplet.attr
        assert(wHist(oldTopic) > 0)
        assert(dHist(oldTopic) > 0)
        assert(totalHist(oldTopic) > 0)
        // Construct the conditional
        val conditional = new Array[Double](nt)
        var t = 0
        var conditionalSum = 0.0
        while (t < conditional.size) {
          val cavityOffset = if (t == oldTopic) 1 else 0
          val w = wHist(t) - cavityOffset
          val d = dHist(t) - cavityOffset
          val total = totalHist(t) - cavityOffset
          conditional(t) = (a + d) * (b + w) / (b * nw + total)
          conditionalSum += conditional(t)
          t += 1
        }
        assert(conditionalSum > 0.0)
        // Generate a random number between [0, conditionalSum)
        val u = gen.nextDouble() * conditionalSum
        assert(u < conditionalSum)
        // Draw the new topic from the multinomial
        t = 0
        var cumsum = conditional(t)
        while(cumsum < u) {
          t += 1
          cumsum += conditional(t)
        }
        val newTopic = t
        // Return the new topic
        newTopic
      }

      // Resample all the tokens
      val parts = graph.edges.partitions.size
      val interIter = internalIteration
      graph = graph.mapTriplets { (pid, iter) =>
        val gen = new java.util.Random(parts * interIter + pid)
        iter.map(token => sampleToken(gen, token))
      }

      // Update the counts
      val newCounts = graph.mapReduceTriplets[Factor](
        e => Iterator((e.srcId, makeFactor(nt, e.attr)), (e.dstId, makeFactor(nt, e.attr))),
        (a, b) => { addEq(a,b); a } )
      graph = graph.outerJoinVertices(newCounts) { (_, _, newFactorOpt) => newFactorOpt.get }.cache

      // Recompute the global counts (the actual action)
      totalHist = graph.edges.map(e => e.attr)
        .aggregate(new Factor(nt))(LDA.addEq(_, _), LDA.addEq(_, _))
      assert(totalHist.sum == nTokens)

      internalIteration += 1
      println("Sampled iteration: " + i.toString)
    }
  } // end of iterate

  def topWords(k: Int): Array[Array[(Count, WordId)]] = {
    val nt = nTopics
    graph.vertices.filter {
      case (vid, c) => vid >= 0
    }.mapPartitions { items =>
      val queues = Array.fill(nt)(new BoundedPriorityQueue[(Count, WordId)](k))
      for ((wordId, factor) <- items) {
        var t = 0
        while (t < nt) {
          val tpl: (Count, WordId) = (factor(t), wordId)
          queues(t) += tpl
          t += 1
        }
      }
      Iterator(queues)
    }.reduce { (q1, q2) =>
      q1.zip(q2).foreach { case (a,b) => a ++= b }
      q1
    }.map ( q => q.toArray )
  } // end of TopWords

  def posterior: Posterior = {
    graph.cache()
    val words = graph.vertices.filter { case (vid, _) => vid >= 0 }
    val docs =  graph.vertices.filter { case (vid,_) => vid < 0 }
    new LDA.Posterior(words, docs)
  }

} // end of TopicModeling



object TopicModeling {
  def main(args: Array[String]) {
    val host = "local" // args(0)
    val options =  args.drop(3).map { arg =>
      arg.dropWhile(_ == '-').split('=') match {
        case Array(opt, v) => (opt -> v)
        case _ => throw new IllegalArgumentException("Invalid argument: " + arg)
      }
    }

    var tokensFile = ""
    var dictionaryFile = ""
    var numVPart = 4
    var numEPart = 4
    var partitionStrategy: Option[PartitionStrategy] = None
    var nIter = 50
    var nTopics = 10
    var alpha = 0.1
    var beta  = 0.1

    def pickPartitioner(v: String): PartitionStrategy = v match {
      case "RandomVertexCut" => RandomVertexCut
      case "EdgePartition1D" => EdgePartition1D
      case "EdgePartition2D" => EdgePartition2D
      case "CanonicalRandomVertexCut" => CanonicalRandomVertexCut
      case _ => throw new IllegalArgumentException("Invalid Partition Strategy: " + v)
    }

    options.foreach{
      case ("tokens", v) => tokensFile = v
      case ("dictionary", v) => dictionaryFile = v
      case ("numVPart", v) => numVPart = v.toInt
      case ("numEPart", v) => numEPart = v.toInt
      case ("partStrategy", v) => partitionStrategy = Some(pickPartitioner(v))
      case ("niter", v) => nIter = v.toInt
      case ("ntopics", v) => nTopics = v.toInt
      case ("alpha", v) => alpha = v.toDouble
      case ("beta", v) => beta = v.toDouble
      case (opt, _) => throw new IllegalArgumentException("Invalid option: " + opt)
    }


    // def setLogLevels(level: org.apache.log4j.Level, loggers: TraversableOnce[String]) = {
    //   loggers.map{
    //     loggerName =>
    //       val logger = org.apache.log4j.Logger.getLogger(loggerName)
    //     val prevLevel = logger.getLevel()
    //     logger.setLevel(level)
    //     loggerName -> prevLevel
    //   }.toMap
    // }
    // setLogLevels(org.apache.log4j.Level.DEBUG, Seq("org.apache.spark"))


    val serializer = "org.apache.spark.serializer.KryoSerializer"
    System.setProperty("spark.serializer", serializer)
    //System.setProperty("spark.shuffle.compress", "false")
    System.setProperty("spark.kryo.registrator", "org.apache.spark.graph.GraphKryoRegistrator")
    val sc = new SparkContext(host, "LDA(" + tokensFile + ")")

    val rawTokens: RDD[(LDA.WordId, LDA.DocId)] =
      sc.textFile(tokensFile, numEPart).flatMap { line =>
      val lineArray = line.split("\\s+")
      if(lineArray.length != 3) {
        println("Invalid line: " + line)
        assert(false)
      }
      val termId = lineArray(0).trim.toLong
      val docId = lineArray(1).trim.toLong
      assert(termId >= 0)
      assert(docId >= 0)
      val count = lineArray(2).trim.toInt
      assert(count > 0)
      //Iterator((termId, docId))
      Iterator.fill(count)((termId, docId))
    }

    val dictionary =
      if (!dictionaryFile.isEmpty) {
        scala.io.Source.fromFile(dictionaryFile).getLines.toArray
      } else {
        Array.empty
      }

    val model = new LDA(rawTokens, nTopics, alpha, beta)

    for(iter <- 0 until nIter) {
      model.iterate(1)
      val topWords = model.topWords(5)
      for (queue <- topWords) {
        println("word list: ")
        if (!dictionary.isEmpty) {
          queue.foreach(t => println("\t(" + t._1 + ", " + dictionary(t._2.toInt - 1) + ")"))
        } else {
          queue.foreach(t => println("\t" + t.toString))
        }
      }
    }

    sc.stop()

  }
} // end of TopicModeling object

