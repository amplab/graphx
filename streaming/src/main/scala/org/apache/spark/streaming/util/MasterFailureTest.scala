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

package org.apache.spark.streaming.util

import org.apache.spark.Logging
import org.apache.spark.rdd.RDD
import org.apache.spark.streaming._
import org.apache.spark.streaming.dstream.ForEachDStream
import StreamingContext._

import scala.util.Random
import scala.collection.mutable.{SynchronizedBuffer, ArrayBuffer}
import scala.reflect.ClassTag

import java.io.{File, ObjectInputStream, IOException}
import java.util.UUID

import com.google.common.io.Files

import org.apache.commons.io.FileUtils
import org.apache.hadoop.fs.{FileUtil, FileSystem, Path}
import org.apache.hadoop.conf.Configuration


private[streaming]
object MasterFailureTest extends Logging {

  @volatile var killed = false
  @volatile var killCount = 0

  def main(args: Array[String]) {
    if (args.size < 2) {
      println(
        "Usage: MasterFailureTest <local/HDFS directory> <# batches> [<batch size in milliseconds>]")
      System.exit(1)
    }
    val directory = args(0)
    val numBatches = args(1).toInt
    val batchDuration = if (args.size > 2) Milliseconds(args(2).toInt) else Seconds(1)

    println("\n\n========================= MAP TEST =========================\n\n")
    testMap(directory, numBatches, batchDuration)

    println("\n\n================= UPDATE-STATE-BY-KEY TEST =================\n\n")
    testUpdateStateByKey(directory, numBatches, batchDuration)

    println("\n\nSUCCESS\n\n")
  }

  def testMap(directory: String, numBatches: Int, batchDuration: Duration) {
    // Input: time=1 ==> [ 1 ] , time=2 ==> [ 2 ] , time=3 ==> [ 3 ] , ...
    val input = (1 to numBatches).map(_.toString).toSeq
    // Expected output: time=1 ==> [ 1 ] , time=2 ==> [ 2 ] , time=3 ==> [ 3 ] , ...
    val expectedOutput = (1 to numBatches)

    val operation = (st: DStream[String]) => st.map(_.toInt)

    // Run streaming operation with multiple master failures
    val output = testOperation(directory, batchDuration, input, operation, expectedOutput)

    logInfo("Expected output, size = " + expectedOutput.size)
    logInfo(expectedOutput.mkString("[", ",", "]"))
    logInfo("Output, size = " + output.size)
    logInfo(output.mkString("[", ",", "]"))

    // Verify whether all the values of the expected output is present
    // in the output
    assert(output.distinct.toSet == expectedOutput.toSet)
  }


  def testUpdateStateByKey(directory: String, numBatches: Int, batchDuration: Duration) {
    // Input: time=1 ==> [ a ] , time=2 ==> [ a, a ] , time=3 ==> [ a, a, a ] , ...
    val input = (1 to numBatches).map(i => (1 to i).map(_ => "a").mkString(" ")).toSeq
    // Expected output: time=1 ==> [ (a, 1) ] , time=2 ==> [ (a, 3) ] , time=3 ==> [ (a,6) ] , ...
    val expectedOutput = (1L to numBatches).map(i => (1L to i).reduce(_ + _)).map(j => ("a", j))

    val operation = (st: DStream[String]) => {
      val updateFunc = (values: Seq[Long], state: Option[Long]) => {
        Some(values.foldLeft(0L)(_ + _) + state.getOrElse(0L))
      }
      st.flatMap(_.split(" "))
        .map(x => (x, 1L))
        .updateStateByKey[Long](updateFunc)
        .checkpoint(batchDuration * 5)
    }

    // Run streaming operation with multiple master failures
    val output = testOperation(directory, batchDuration, input, operation, expectedOutput)

    logInfo("Expected output, size = " + expectedOutput.size + "\n" + expectedOutput)
    logInfo("Output, size = " + output.size + "\n" + output)

    // Verify whether all the values in the output are among the expected output values
    output.foreach(o =>
      assert(expectedOutput.contains(o), "Expected value " + o + " not found")
    )

    // Verify whether the last expected output value has been generated, there by
    // confirming that none of the inputs have been missed
    assert(output.last == expectedOutput.last)
  }

  /**
   * Tests stream operation with multiple master failures, and verifies whether the
   * final set of output values is as expected or not.
   */
  def testOperation[T: ClassTag](
    directory: String,
    batchDuration: Duration,
    input: Seq[String],
    operation: DStream[String] => DStream[T],
    expectedOutput: Seq[T]
  ): Seq[T] = {

    // Just making sure that the expected output does not have duplicates
    assert(expectedOutput.distinct.toSet == expectedOutput.toSet)

    // Setup the stream computation with the given operation
    val (ssc, checkpointDir, testDir) = setupStreams(directory, batchDuration, operation)

    // Start generating files in the a different thread
    val fileGeneratingThread = new FileGeneratingThread(input, testDir, batchDuration.milliseconds)
    fileGeneratingThread.start()

    // Run the streams and repeatedly kill it until the last expected output
    // has been generated, or until it has run for twice the expected time
    val lastExpectedOutput = expectedOutput.last
    val maxTimeToRun = expectedOutput.size * batchDuration.milliseconds * 2
    val mergedOutput = runStreams(ssc, lastExpectedOutput, maxTimeToRun)

    // Delete directories
    fileGeneratingThread.join()
    val fs = checkpointDir.getFileSystem(new Configuration())
    fs.delete(checkpointDir, true)
    fs.delete(testDir, true)
    logInfo("Finished test after " + killCount + " failures")
    mergedOutput
  }

  /**
   * Sets up the stream computation with the given operation, directory (local or HDFS),
   * and batch duration. Returns the streaming context and the directory to which
   * files should be written for testing.
   */
  private def setupStreams[T: ClassTag](
      directory: String,
      batchDuration: Duration,
      operation: DStream[String] => DStream[T]
    ): (StreamingContext, Path, Path) = {
    // Reset all state
    reset()

    // Create the directories for this test
    val uuid = UUID.randomUUID().toString
    val rootDir = new Path(directory, uuid)
    val fs = rootDir.getFileSystem(new Configuration())
    val checkpointDir = new Path(rootDir, "checkpoint")
    val testDir = new Path(rootDir, "test")
    fs.mkdirs(checkpointDir)
    fs.mkdirs(testDir)

    // Setup the streaming computation with the given operation
    System.clearProperty("spark.driver.port")
    System.clearProperty("spark.hostPort")
    var ssc = new StreamingContext("local[4]", "MasterFailureTest", batchDuration, null, Nil, Map())
    ssc.checkpoint(checkpointDir.toString)
    val inputStream = ssc.textFileStream(testDir.toString)
    val operatedStream = operation(inputStream)
    val outputStream = new TestOutputStream(operatedStream)
    ssc.registerOutputStream(outputStream)
    (ssc, checkpointDir, testDir)
  }


  /**
   * Repeatedly starts and kills the streaming context until timed out or
   * the last expected output is generated. Finally, return
   */
  private def runStreams[T: ClassTag](
      ssc_ : StreamingContext,
      lastExpectedOutput: T,
      maxTimeToRun: Long
   ): Seq[T] = {

    var ssc = ssc_
    var totalTimeRan = 0L
    var isLastOutputGenerated = false
    var isTimedOut = false
    val mergedOutput = new ArrayBuffer[T]()
    val checkpointDir = ssc.checkpointDir
    var batchDuration = ssc.graph.batchDuration

    while(!isLastOutputGenerated && !isTimedOut) {
      // Get the output buffer
      val outputBuffer = ssc.graph.getOutputStreams.head.asInstanceOf[TestOutputStream[T]].output
      def output = outputBuffer.flatMap(x => x)

      // Start the thread to kill the streaming after some time
      killed = false
      val killingThread = new KillingThread(ssc, batchDuration.milliseconds * 10)
      killingThread.start()

      var timeRan = 0L
      try {
        // Start the streaming computation and let it run while ...
        // (i) StreamingContext has not been shut down yet
        // (ii) The last expected output has not been generated yet
        // (iii) Its not timed out yet
        System.clearProperty("spark.streaming.clock")
        System.clearProperty("spark.driver.port")
        System.clearProperty("spark.hostPort")
        ssc.start()
        val startTime = System.currentTimeMillis()
        while (!killed && !isLastOutputGenerated && !isTimedOut) {
          Thread.sleep(100)
          timeRan = System.currentTimeMillis() - startTime
          isLastOutputGenerated = (!output.isEmpty && output.last == lastExpectedOutput)
          isTimedOut = (timeRan + totalTimeRan > maxTimeToRun)
        }
      } catch {
        case e: Exception => logError("Error running streaming context", e)
      }
      if (killingThread.isAlive) killingThread.interrupt()
      ssc.stop()

      logInfo("Has been killed = " + killed)
      logInfo("Is last output generated = " + isLastOutputGenerated)
      logInfo("Is timed out = " + isTimedOut)

      // Verify whether the output of each batch has only one element or no element
      // and then merge the new output with all the earlier output
      mergedOutput ++= output
      totalTimeRan += timeRan
      logInfo("New output = " + output)
      logInfo("Merged output = " + mergedOutput)
      logInfo("Time ran = " + timeRan)
      logInfo("Total time ran = " + totalTimeRan)

      if (!isLastOutputGenerated && !isTimedOut) {
        val sleepTime = Random.nextInt(batchDuration.milliseconds.toInt * 10)
        logInfo(
          "\n-------------------------------------------\n" +
            "   Restarting stream computation in " + sleepTime + " ms   " +
            "\n-------------------------------------------\n"
        )
        Thread.sleep(sleepTime)
        // Recreate the streaming context from checkpoint
        ssc = new StreamingContext(checkpointDir)
      }
    }
    mergedOutput
  }

  /**
   * Verifies the output value are the same as expected. Since failures can lead to
   * a batch being processed twice, a batches output may appear more than once
   * consecutively. To avoid getting confused with those, we eliminate consecutive
   * duplicate batch outputs of values from the `output`. As a result, the
   * expected output should not have consecutive batches with the same values as output.
   */
  private def verifyOutput[T: ClassTag](output: Seq[T], expectedOutput: Seq[T]) {
    // Verify whether expected outputs do not consecutive batches with same output
    for (i <- 0 until expectedOutput.size - 1) {
      assert(expectedOutput(i) != expectedOutput(i+1),
        "Expected output has consecutive duplicate sequence of values")
    }

    // Log the output
    println("Expected output, size = " + expectedOutput.size)
    println(expectedOutput.mkString("[", ",", "]"))
    println("Output, size = " + output.size)
    println(output.mkString("[", ",", "]"))

    // Match the output with the expected output
    output.foreach(o =>
      assert(expectedOutput.contains(o), "Expected value " + o + " not found")
    )
  }

  /** Resets counter to prepare for the test */
  private def reset() {
    killed = false
    killCount = 0
  }
}

/**
 * This is a output stream just for testing. All the output is collected into a
 * ArrayBuffer. This buffer is wiped clean on being restored from checkpoint.
 */
private[streaming]
class TestOutputStream[T: ClassTag](
    parent: DStream[T],
    val output: ArrayBuffer[Seq[T]] = new ArrayBuffer[Seq[T]] with SynchronizedBuffer[Seq[T]]
  ) extends ForEachDStream[T](
    parent,
    (rdd: RDD[T], t: Time) => {
      val collected = rdd.collect()
      output += collected
    }
  ) {

  // This is to clear the output buffer every it is read from a checkpoint
  @throws(classOf[IOException])
  private def readObject(ois: ObjectInputStream) {
    ois.defaultReadObject()
    output.clear()
  }
}


/**
 * Thread to kill streaming context after a random period of time.
 */
private[streaming]
class KillingThread(ssc: StreamingContext, maxKillWaitTime: Long) extends Thread with Logging {

  override def run() {
    try {
      // If it is the first killing, then allow the first checkpoint to be created
      var minKillWaitTime = if (MasterFailureTest.killCount == 0) 5000 else 2000
      val killWaitTime = minKillWaitTime + math.abs(Random.nextLong % maxKillWaitTime)
      logInfo("Kill wait time = " + killWaitTime)
      Thread.sleep(killWaitTime)
      logInfo(
        "\n---------------------------------------\n" +
          "Killing streaming context after " + killWaitTime + " ms" +
          "\n---------------------------------------\n"
      )
      if (ssc != null) {
        ssc.stop()
        MasterFailureTest.killed = true
        MasterFailureTest.killCount += 1
      }
      logInfo("Killing thread finished normally")
    } catch {
      case ie: InterruptedException => logInfo("Killing thread interrupted")
      case e: Exception => logWarning("Exception in killing thread", e)
    }

  }
}


/**
 * Thread to generate input files periodically with the desired text.
 */
private[streaming]
class FileGeneratingThread(input: Seq[String], testDir: Path, interval: Long)
  extends Thread with Logging {

  override def run() {
    val localTestDir = Files.createTempDir()
    var fs = testDir.getFileSystem(new Configuration())
    val maxTries = 3
    try {
      Thread.sleep(5000) // To make sure that all the streaming context has been set up
      for (i <- 0 until input.size) {
        // Write the data to a local file and then move it to the target test directory
        val localFile = new File(localTestDir, (i+1).toString)
        val hadoopFile = new Path(testDir, (i+1).toString)
        val tempHadoopFile = new Path(testDir, ".tmp_" + (i+1).toString)
        FileUtils.writeStringToFile(localFile, input(i).toString + "\n")
        var tries = 0
        var done = false
            while (!done && tries < maxTries) {
              tries += 1
              try {
                // fs.copyFromLocalFile(new Path(localFile.toString), hadoopFile)
                fs.copyFromLocalFile(new Path(localFile.toString), tempHadoopFile)
                fs.rename(tempHadoopFile, hadoopFile)
            done = true
          } catch {
            case ioe: IOException => {
                  fs = testDir.getFileSystem(new Configuration())
                  logWarning("Attempt " + tries + " at generating file " + hadoopFile + " failed.", ioe)
            }
          }
        }
    if (!done)
          logError("Could not generate file " + hadoopFile)
        else
          logInfo("Generated file " + hadoopFile + " at " + System.currentTimeMillis)
        Thread.sleep(interval)
        localFile.delete()
      }
      logInfo("File generating thread finished normally")
    } catch {
      case ie: InterruptedException => logInfo("File generating thread interrupted")
      case e: Exception => logWarning("File generating in killing thread", e)
    } finally {
      fs.close()
    }
  }
}
