/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.spark.streaming.api.java


import scala.collection.JavaConversions._
import scala.reflect.ClassTag

import java.io.InputStream
import java.lang.{Integer => JInt}
import java.util.{List => JList, Map => JMap}

import akka.actor.{Props, SupervisorStrategy}
import org.apache.hadoop.mapreduce.{InputFormat => NewInputFormat}

import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.api.java.{JavaPairRDD, JavaRDD, JavaSparkContext}
import org.apache.spark.api.java.function.{Function => JFunction, Function2 => JFunction2}
import org.apache.spark.rdd.RDD
import org.apache.spark.storage.StorageLevel
import org.apache.spark.streaming._
import org.apache.spark.streaming.scheduler.StreamingListener

/**
 * A StreamingContext is the main entry point for Spark Streaming functionality. Besides the basic
 * information (such as, cluster URL and job name) to internally create a SparkContext, it provides
 * methods used to create DStream from various input sources.
 */
class JavaStreamingContext(val ssc: StreamingContext) {

  // TODOs:
  // - Test to/from Hadoop functions
  // - Support creating and registering InputStreams


  /**
   * Creates a StreamingContext.
   * @param master Name of the Spark Master
   * @param appName Name to be used when registering with the scheduler
   * @param batchDuration The time interval at which streaming data will be divided into batches
   */
  def this(master: String, appName: String, batchDuration: Duration) =
    this(new StreamingContext(master, appName, batchDuration, null, Nil, Map()))

  /**
   * Creates a StreamingContext.
   * @param master Name of the Spark Master
   * @param appName Name to be used when registering with the scheduler
   * @param batchDuration The time interval at which streaming data will be divided into batches
   * @param sparkHome The SPARK_HOME directory on the slave nodes
   * @param jarFile JAR file containing job code, to ship to cluster. This can be a path on the local
   *                file system or an HDFS, HTTP, HTTPS, or FTP URL.
   */
  def this(
      master: String,
      appName: String,
      batchDuration: Duration,
      sparkHome: String,
      jarFile: String) =
    this(new StreamingContext(master, appName, batchDuration, sparkHome, Seq(jarFile), Map()))

  /**
   * Creates a StreamingContext.
   * @param master Name of the Spark Master
   * @param appName Name to be used when registering with the scheduler
   * @param batchDuration The time interval at which streaming data will be divided into batches
   * @param sparkHome The SPARK_HOME directory on the slave nodes
   * @param jars Collection of JARs to send to the cluster. These can be paths on the local file
   *             system or HDFS, HTTP, HTTPS, or FTP URLs.
   */
  def this(
      master: String,
      appName: String,
      batchDuration: Duration,
      sparkHome: String,
      jars: Array[String]) =
    this(new StreamingContext(master, appName, batchDuration, sparkHome, jars, Map()))

  /**
   * Creates a StreamingContext.
   * @param master Name of the Spark Master
   * @param appName Name to be used when registering with the scheduler
   * @param batchDuration The time interval at which streaming data will be divided into batches
   * @param sparkHome The SPARK_HOME directory on the slave nodes
   * @param jars Collection of JARs to send to the cluster. These can be paths on the local file
   *             system or HDFS, HTTP, HTTPS, or FTP URLs.
   * @param environment Environment variables to set on worker nodes
   */
  def this(
    master: String,
    appName: String,
    batchDuration: Duration,
    sparkHome: String,
    jars: Array[String],
    environment: JMap[String, String]) =
    this(new StreamingContext(master, appName, batchDuration, sparkHome, jars, environment))

  /**
   * Creates a StreamingContext using an existing SparkContext.
   * @param sparkContext The underlying JavaSparkContext to use
   * @param batchDuration The time interval at which streaming data will be divided into batches
   */
  def this(sparkContext: JavaSparkContext, batchDuration: Duration) =
    this(new StreamingContext(sparkContext.sc, batchDuration))

  /**
   * Creates a StreamingContext using an existing SparkContext.
   * @param conf A Spark application configuration
   * @param batchDuration The time interval at which streaming data will be divided into batches
   */
  def this(conf: SparkConf, batchDuration: Duration) =
    this(new StreamingContext(conf, batchDuration))

  /**
   * Re-creates a StreamingContext from a checkpoint file.
   * @param path Path either to the directory that was specified as the checkpoint directory, or
   *             to the checkpoint file 'graph' or 'graph.bk'.
   */
  def this(path: String) = this (new StreamingContext(path))

  /** The underlying SparkContext */
  val sc: JavaSparkContext = new JavaSparkContext(ssc.sc)

  /**
   * Create a input stream from network source hostname:port. Data is received using
   * a TCP socket and the receive bytes is interpreted as UTF8 encoded \n delimited
   * lines.
   * @param hostname      Hostname to connect to for receiving data
   * @param port          Port to connect to for receiving data
   * @param storageLevel  Storage level to use for storing the received objects
   *                      (default: StorageLevel.MEMORY_AND_DISK_SER_2)
   */
  def socketTextStream(hostname: String, port: Int, storageLevel: StorageLevel)
  : JavaDStream[String] = {
    ssc.socketTextStream(hostname, port, storageLevel)
  }

  /**
   * Create a input stream from network source hostname:port. Data is received using
   * a TCP socket and the receive bytes is interpreted as UTF8 encoded \n delimited
   * lines.
   * @param hostname      Hostname to connect to for receiving data
   * @param port          Port to connect to for receiving data
   */
  def socketTextStream(hostname: String, port: Int): JavaDStream[String] = {
    ssc.socketTextStream(hostname, port)
  }

  /**
   * Create a input stream from network source hostname:port. Data is received using
   * a TCP socket and the receive bytes it interepreted as object using the given
   * converter.
   * @param hostname      Hostname to connect to for receiving data
   * @param port          Port to connect to for receiving data
   * @param converter     Function to convert the byte stream to objects
   * @param storageLevel  Storage level to use for storing the received objects
   * @tparam T            Type of the objects received (after converting bytes to objects)
   */
  def socketStream[T](
      hostname: String,
      port: Int,
      converter: JFunction[InputStream, java.lang.Iterable[T]],
      storageLevel: StorageLevel)
  : JavaDStream[T] = {
    def fn = (x: InputStream) => converter.apply(x).toIterator
    implicit val cmt: ClassTag[T] =
      implicitly[ClassTag[AnyRef]].asInstanceOf[ClassTag[T]]
    ssc.socketStream(hostname, port, fn, storageLevel)
  }

  /**
   * Create a input stream that monitors a Hadoop-compatible filesystem
   * for new files and reads them as text files (using key as LongWritable, value
   * as Text and input format as TextInputFormat). Files must be written to the
   * monitored directory by "moving" them from another location within the same
   * file system. File names starting with . are ignored.
   * @param directory HDFS directory to monitor for new file
   */
  def textFileStream(directory: String): JavaDStream[String] = {
    ssc.textFileStream(directory)
  }

  /**
   * Create a input stream from network source hostname:port, where data is received
   * as serialized blocks (serialized using the Spark's serializer) that can be directly
   * pushed into the block manager without deserializing them. This is the most efficient
   * way to receive data.
   * @param hostname      Hostname to connect to for receiving data
   * @param port          Port to connect to for receiving data
   * @param storageLevel  Storage level to use for storing the received objects
   * @tparam T            Type of the objects in the received blocks
   */
  def rawSocketStream[T](
      hostname: String,
      port: Int,
      storageLevel: StorageLevel): JavaDStream[T] = {
    implicit val cmt: ClassTag[T] =
      implicitly[ClassTag[AnyRef]].asInstanceOf[ClassTag[T]]
    JavaDStream.fromDStream(ssc.rawSocketStream(hostname, port, storageLevel))
  }

  /**
   * Create a input stream from network source hostname:port, where data is received
   * as serialized blocks (serialized using the Spark's serializer) that can be directly
   * pushed into the block manager without deserializing them. This is the most efficient
   * way to receive data.
   * @param hostname      Hostname to connect to for receiving data
   * @param port          Port to connect to for receiving data
   * @tparam T            Type of the objects in the received blocks
   */
  def rawSocketStream[T](hostname: String, port: Int): JavaDStream[T] = {
    implicit val cmt: ClassTag[T] =
      implicitly[ClassTag[AnyRef]].asInstanceOf[ClassTag[T]]
    JavaDStream.fromDStream(ssc.rawSocketStream(hostname, port))
  }

  /**
   * Create a input stream that monitors a Hadoop-compatible filesystem
   * for new files and reads them using the given key-value types and input format.
   * Files must be written to the monitored directory by "moving" them from another
   * location within the same file system. File names starting with . are ignored.
   * @param directory HDFS directory to monitor for new file
   * @tparam K Key type for reading HDFS file
   * @tparam V Value type for reading HDFS file
   * @tparam F Input format for reading HDFS file
   */
  def fileStream[K, V, F <: NewInputFormat[K, V]](directory: String): JavaPairDStream[K, V] = {
    implicit val cmk: ClassTag[K] =
      implicitly[ClassTag[AnyRef]].asInstanceOf[ClassTag[K]]
    implicit val cmv: ClassTag[V] =
      implicitly[ClassTag[AnyRef]].asInstanceOf[ClassTag[V]]
    implicit val cmf: ClassTag[F] =
      implicitly[ClassTag[AnyRef]].asInstanceOf[ClassTag[F]]
    ssc.fileStream[K, V, F](directory)
  }

  /**
   * Create an input stream with any arbitrary user implemented actor receiver.
   * @param props Props object defining creation of the actor
   * @param name Name of the actor
   * @param storageLevel Storage level to use for storing the received objects
   *
   * @note An important point to note:
   *       Since Actor may exist outside the spark framework, It is thus user's responsibility
   *       to ensure the type safety, i.e parametrized type of data received and actorStream
   *       should be same.
   */
  def actorStream[T](
      props: Props,
      name: String,
      storageLevel: StorageLevel,
      supervisorStrategy: SupervisorStrategy
    ): JavaDStream[T] = {
    implicit val cm: ClassTag[T] =
      implicitly[ClassTag[AnyRef]].asInstanceOf[ClassTag[T]]
    ssc.actorStream[T](props, name, storageLevel, supervisorStrategy)
  }

  /**
   * Create an input stream with any arbitrary user implemented actor receiver.
   * @param props Props object defining creation of the actor
   * @param name Name of the actor
   * @param storageLevel Storage level to use for storing the received objects
   *
   * @note An important point to note:
   *       Since Actor may exist outside the spark framework, It is thus user's responsibility
   *       to ensure the type safety, i.e parametrized type of data received and actorStream
   *       should be same.
   */
  def actorStream[T](
      props: Props,
      name: String,
      storageLevel: StorageLevel
  ): JavaDStream[T] = {
    implicit val cm: ClassTag[T] =
      implicitly[ClassTag[AnyRef]].asInstanceOf[ClassTag[T]]
    ssc.actorStream[T](props, name, storageLevel)
  }

  /**
   * Create an input stream with any arbitrary user implemented actor receiver.
   * @param props Props object defining creation of the actor
   * @param name Name of the actor
   *
   * @note An important point to note:
   *       Since Actor may exist outside the spark framework, It is thus user's responsibility
   *       to ensure the type safety, i.e parametrized type of data received and actorStream
   *       should be same.
   */
  def actorStream[T](
      props: Props,
      name: String
    ): JavaDStream[T] = {
    implicit val cm: ClassTag[T] =
      implicitly[ClassTag[AnyRef]].asInstanceOf[ClassTag[T]]
    ssc.actorStream[T](props, name)
  }

  /**
   * Registers an output stream that will be computed every interval
   */
  def registerOutputStream(outputStream: JavaDStreamLike[_, _, _]) {
    ssc.registerOutputStream(outputStream.dstream)
  }

  /**
   * Creates a input stream from an queue of RDDs. In each batch,
   * it will process either one or all of the RDDs returned by the queue.
   *
   * NOTE: changes to the queue after the stream is created will not be recognized.
   * @param queue      Queue of RDDs
   * @tparam T         Type of objects in the RDD
   */
  def queueStream[T](queue: java.util.Queue[JavaRDD[T]]): JavaDStream[T] = {
    implicit val cm: ClassTag[T] =
      implicitly[ClassTag[AnyRef]].asInstanceOf[ClassTag[T]]
    val sQueue = new scala.collection.mutable.Queue[RDD[T]]
    sQueue.enqueue(queue.map(_.rdd).toSeq: _*)
    ssc.queueStream(sQueue)
  }

  /**
   * Creates a input stream from an queue of RDDs. In each batch,
   * it will process either one or all of the RDDs returned by the queue.
   *
   * NOTE: changes to the queue after the stream is created will not be recognized.
   * @param queue      Queue of RDDs
   * @param oneAtATime Whether only one RDD should be consumed from the queue in every interval
   * @tparam T         Type of objects in the RDD
   */
  def queueStream[T](queue: java.util.Queue[JavaRDD[T]], oneAtATime: Boolean): JavaDStream[T] = {
    implicit val cm: ClassTag[T] =
      implicitly[ClassTag[AnyRef]].asInstanceOf[ClassTag[T]]
    val sQueue = new scala.collection.mutable.Queue[RDD[T]]
    sQueue.enqueue(queue.map(_.rdd).toSeq: _*)
    ssc.queueStream(sQueue, oneAtATime)
  }

  /**
   * Creates a input stream from an queue of RDDs. In each batch,
   * it will process either one or all of the RDDs returned by the queue.
   *
   * NOTE: changes to the queue after the stream is created will not be recognized.
   * @param queue      Queue of RDDs
   * @param oneAtATime Whether only one RDD should be consumed from the queue in every interval
   * @param defaultRDD Default RDD is returned by the DStream when the queue is empty
   * @tparam T         Type of objects in the RDD
   */
  def queueStream[T](
      queue: java.util.Queue[JavaRDD[T]],
      oneAtATime: Boolean,
      defaultRDD: JavaRDD[T]): JavaDStream[T] = {
    implicit val cm: ClassTag[T] =
      implicitly[ClassTag[AnyRef]].asInstanceOf[ClassTag[T]]
    val sQueue = new scala.collection.mutable.Queue[RDD[T]]
    sQueue.enqueue(queue.map(_.rdd).toSeq: _*)
    ssc.queueStream(sQueue, oneAtATime, defaultRDD.rdd)
  }

  /**
   * Create a unified DStream from multiple DStreams of the same type and same slide duration.
   */
  def union[T](first: JavaDStream[T], rest: JList[JavaDStream[T]]): JavaDStream[T] = {
    val dstreams: Seq[DStream[T]] = (Seq(first) ++ asScalaBuffer(rest)).map(_.dstream)
    implicit val cm: ClassTag[T] = first.classTag
    ssc.union(dstreams)(cm)
  }

  /**
   * Create a unified DStream from multiple DStreams of the same type and same slide duration.
   */
  def union[K, V](
      first: JavaPairDStream[K, V],
      rest: JList[JavaPairDStream[K, V]]
    ): JavaPairDStream[K, V] = {
    val dstreams: Seq[DStream[(K, V)]] = (Seq(first) ++ asScalaBuffer(rest)).map(_.dstream)
    implicit val cm: ClassTag[(K, V)] = first.classTag
    implicit val kcm: ClassTag[K] = first.kManifest
    implicit val vcm: ClassTag[V] = first.vManifest
    new JavaPairDStream[K, V](ssc.union(dstreams)(cm))(kcm, vcm)
  }

  /**
   * Create a new DStream in which each RDD is generated by applying a function on RDDs of
   * the DStreams. The order of the JavaRDDs in the transform function parameter will be the
   * same as the order of corresponding DStreams in the list. Note that for adding a
   * JavaPairDStream in the list of JavaDStreams, convert it to a JavaDStream using
   * [[org.apache.spark.streaming.api.java.JavaPairDStream]].toJavaDStream().
   * In the transform function, convert the JavaRDD corresponding to that JavaDStream to
   * a JavaPairRDD using [[org.apache.spark.api.java.JavaPairRDD]].fromJavaRDD().
   */
  def transform[T](
      dstreams: JList[JavaDStream[_]],
      transformFunc: JFunction2[JList[JavaRDD[_]], Time, JavaRDD[T]]
    ): JavaDStream[T] = {
    implicit val cmt: ClassTag[T] =
      implicitly[ClassTag[AnyRef]].asInstanceOf[ClassTag[T]]
    val scalaDStreams = dstreams.map(_.dstream).toSeq
    val scalaTransformFunc = (rdds: Seq[RDD[_]], time: Time) => {
      val jrdds = rdds.map(rdd => JavaRDD.fromRDD[AnyRef](rdd.asInstanceOf[RDD[AnyRef]])).toList
      transformFunc.call(jrdds, time).rdd
    }
    ssc.transform(scalaDStreams, scalaTransformFunc)
  }

  /**
   * Create a new DStream in which each RDD is generated by applying a function on RDDs of
   * the DStreams. The order of the JavaRDDs in the transform function parameter will be the
   * same as the order of corresponding DStreams in the list. Note that for adding a
   * JavaPairDStream in the list of JavaDStreams, convert it to a JavaDStream using
   * [[org.apache.spark.streaming.api.java.JavaPairDStream]].toJavaDStream().
   * In the transform function, convert the JavaRDD corresponding to that JavaDStream to
   * a JavaPairRDD using [[org.apache.spark.api.java.JavaPairRDD]].fromJavaRDD().
   */
  def transform[K, V](
      dstreams: JList[JavaDStream[_]],
      transformFunc: JFunction2[JList[JavaRDD[_]], Time, JavaPairRDD[K, V]]
    ): JavaPairDStream[K, V] = {
    implicit val cmk: ClassTag[K] =
      implicitly[ClassTag[AnyRef]].asInstanceOf[ClassTag[K]]
    implicit val cmv: ClassTag[V] =
      implicitly[ClassTag[AnyRef]].asInstanceOf[ClassTag[V]]
    val scalaDStreams = dstreams.map(_.dstream).toSeq
    val scalaTransformFunc = (rdds: Seq[RDD[_]], time: Time) => {
      val jrdds = rdds.map(rdd => JavaRDD.fromRDD[AnyRef](rdd.asInstanceOf[RDD[AnyRef]])).toList
      transformFunc.call(jrdds, time).rdd
    }
    ssc.transform(scalaDStreams, scalaTransformFunc)
  }

  /**
   * Sets the context to periodically checkpoint the DStream operations for master
   * fault-tolerance. The graph will be checkpointed every batch interval.
   * @param directory HDFS-compatible directory where the checkpoint data will be reliably stored
   */
  def checkpoint(directory: String) {
    ssc.checkpoint(directory)
  }

  /**
   * Sets each DStreams in this context to remember RDDs it generated in the last given duration.
   * DStreams remember RDDs only for a limited duration of duration and releases them for garbage
   * collection. This method allows the developer to specify how to long to remember the RDDs (
   * if the developer wishes to query old data outside the DStream computation).
   * @param duration Minimum duration that each DStream should remember its RDDs
   */
  def remember(duration: Duration) {
    ssc.remember(duration)
  }

  /** Add a [[org.apache.spark.streaming.scheduler.StreamingListener]] object for
    * receiving system events related to streaming.
    */
  def addStreamingListener(streamingListener: StreamingListener) {
    ssc.addStreamingListener(streamingListener)
  }

  /**
   * Starts the execution of the streams.
   */
  def start() = ssc.start()

  /**
   * Sstops the execution of the streams.
   */
  def stop() = ssc.stop()
}

object JavaStreamingContext {
  /**
   * Find the JAR from which a given class was loaded, to make it easy for users to pass
   * their JARs to SparkContext.
   */
  def jarOfClass(cls: Class[_]) = SparkContext.jarOfClass(cls).toArray
}
