package org.apache.spark.graph.algorithms

import util.Random
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

  class Posterior (docs: VertexRDD[Array[Count]], words: VertexRDD[Array[Count]])
} // end of LDA singleton


class LDA(@transient val tokens: RDD[(LDA.WordId, LDA.DocId)],
          val nTopics: Int = 100,
          val alpha: Double = 0.1,
          val beta: Double = 0.1) {
  import LDA._

  /**
   * The Factor class represents a vector of counts that is nTopics long.
   *
   * The factor class internally stores a single topic assignment (e.g., [0 0 0 1 0 0]) as
   * a scalar (i.e., topicId = 3) rather than allocating an array.  This optimization is
   * needed to efficienlty support adding token assignments to count vectors.
   *
   * The Factor class is a member of the LDA class because it relies on the nTopics member
   * variable.
   *
   * @param topic
   * @param counts
   */
  class Factor(private var topic: TopicId = -1, private var counts: Array[Count] = null) {
    def this(topic: TopicId) = this(topic, null)
    def this(counts: Array[Count]) = this(-1, counts)
    def makeDense() {
      if (counts == null) {
        counts = new Array[Count](nTopics)
        if (topic >= 0) counts(topic) = 1
        topic = -1
      }
    }
    def +=(other: Factor) {
      makeDense()
      if (other.counts == null) {
        counts(topic) += 1
      } else {
        var i = 0
        while (i < other.counts.size) {
          counts(i) += other.counts(i)
          i += 1
        }
      }
    }
    def asCounts(): Array[Count] = {
      makeDense()
      counts
    }
  } // end of Factor


  private val sc = tokens.sparkContext

  /**
   * The bipartite terms by document graph.
   */
  @transient var graph: Graph[Factor, TopicId] = {
    // To setup a bipartite graph it is necessary to ensure that the document and
    // word ids are in a different namespace
    val renumbered = tokens.map { case (wordId, docId) =>
      assert(wordId >= 0)
      assert(docId >= 0)
      val newDocId: DocId = -(docId + 1)
      (wordId, newDocId)
    }
    Graph.fromEdgeTuples(renumbered, new Factor).mapEdges(e => Random.nextInt(nTopics)).cache
  }


  /**
   * The number of unique words in the corpus
   */
  val nwords = graph.vertices.filter{ case (vid, c) => vid >= 0 }.count()

  /**
   * The number of documents in the corpus
   */
  val ndocs = graph.vertices.filter{ case (vid, c) => vid < 0 }.count()

  /**
   * The total counts for each topic
   */
  var topicC: Broadcast[Array[Count]] = null

  // Execute update counts after initializing all the member
  updateCounts()


  /**
   * The update counts function updates the term and document counts in the
   * graph as well as the overall topic count based on the current topic
   * assignments of each token (the edge attributes).
   *
   */
  def updateCounts() {
    implicit object FactorAccumParam extends AccumulatorParam[Factor] {
      def addInPlace(a: Factor, b: Factor): Factor = { a += b; a }
      def zero(initialValue: Factor): Factor = new Factor()
    }
    val accum = sc.accumulator(new Factor())

    def mapFun(e: EdgeTriplet[Factor, TopicId]): Iterator[(Vid, Factor)] = {
      val f = new Factor(e.attr)
      accum += f
      Iterator((e.srcId, f), (e.dstId, f))
    }
    val newCounts = graph.mapReduceTriplets[Factor](mapFun, (a, b) => { a += b; a } )
    graph = graph.outerJoinVertices(newCounts) { (vid, oldFactor, newFactorOpt) => newFactorOpt.get }
    // Trigger computation of the topic counts
    // TODO: We should uncache the graph at some point.
    graph.cache
    graph.vertices.foreach(x => ())
    val globalCounts: Factor = accum.value
    topicC = sc.broadcast(globalCounts.asCounts())
  } // end of update counts

  def topWords(k: Int): Array[Array[(Count, WordId)]] = {
    graph.vertices.filter {
      case (vid, c) => vid >= 0
    }.mapPartitions { items =>
      val queues = Array.fill(nTopics)(new BoundedPriorityQueue[(Count, WordId)](k))
      for ((wordId, factor) <- items) {
        var t = 0
        val counts: Array[Count] = factor.asCounts()
        while (t < nTopics) {
          val tpl: (Count, WordId) = (counts(t), wordId)
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

  /**
   * Run the gibbs sampler
   * @param nIter
   * @return
   */
  def iterate(nIter: Int = 1) {
    // Run the sampling
    for (i <- 0 until nIter) {
      // Resample all the tokens
      graph = graph.mapTriplets {
        triplet => {
          val w: Array[Count] = triplet.srcAttr.asCounts()
          val d: Array[Count] = triplet.dstAttr.asCounts()
          val total: Array[Count] = topicC.value
          val oldTopic = triplet.attr
          // Subtract out the old assignment from the counts
          w(oldTopic) -= 1
          d(oldTopic) -= 1
          total(oldTopic) -= 1
          assert(w(oldTopic) >= 0)
          assert(d(oldTopic) >= 0)
          assert(total(oldTopic) >= 0)
          // Construct the conditional
          val conditional = new Array[Double](nTopics)
          var t = 0
          var conditionalSum = 0.0
          while (t < conditional.size) {
            conditional(t) = (alpha + d(t)) * (beta * w(t)) / (beta * nwords + total(t))
            conditionalSum += conditional(t)
            t += 1
          }
          assert(conditionalSum > 0.0)
          t = 0
          // Generate a random number between [0, conditionalSum)
          val u = Random.nextDouble() * conditionalSum
          assert(u < conditionalSum)
          // Draw the new topic from the multinomial
          t = 0
          var cumsum = conditional(t)
          while(cumsum < u) {
            t += 1
            cumsum += conditional(t)
          }
          val newTopic = t
          // Cheat !!!! and modify the vertex and edge attributes in place
          w(newTopic) += 1
          d(newTopic) += 1
          total(newTopic) += 1 // <-- This might be dangerous
          // Return the new topic
          newTopic
        } // End of resample edge udf
      } // end of map triplets
      updateCounts() // <-- This is an action
      println("Sampled iteration: " + i.toString)
    }
  }

  def posterior: Posterior = {
    graph.cache()
    val words = graph.vertices.filter { case (vid, _) => vid >= 0 }.mapValues(_.asCounts())
    val docs =  graph.vertices.filter { case (vid,_) => vid < 0 }.mapValues(_.asCounts())
    new LDA.Posterior(words, docs)
  }

} // end of TopicModeling
