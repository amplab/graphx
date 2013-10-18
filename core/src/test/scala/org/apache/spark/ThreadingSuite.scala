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

package org.apache.spark

import java.util.concurrent.Semaphore
import java.util.concurrent.atomic.AtomicBoolean
import java.util.concurrent.atomic.AtomicInteger

import org.scalatest.FunSuite
import org.scalatest.BeforeAndAfter

import SparkContext._

/**
 * Holds state shared across task threads in some ThreadingSuite tests.
 */
object ThreadingSuiteState {
  val runningThreads = new AtomicInteger
  val failed = new AtomicBoolean

  def clear() {
    runningThreads.set(0)
    failed.set(false)
  }
}

class ThreadingSuite extends FunSuite with LocalSparkContext {

  test("accessing SparkContext form a different thread") {
    sc = new SparkContext("local", "test")
    val nums = sc.parallelize(1 to 10, 2)
    val sem = new Semaphore(0)
    @volatile var answer1: Int = 0
    @volatile var answer2: Int = 0
    new Thread {
      override def run() {
        answer1 = nums.reduce(_ + _)
        answer2 = nums.first()    // This will run "locally" in the current thread
        sem.release()
      }
    }.start()
    sem.acquire()
    assert(answer1 === 55)
    assert(answer2 === 1)
  }

  test("accessing SparkContext form multiple threads") {
    sc = new SparkContext("local", "test")
    val nums = sc.parallelize(1 to 10, 2)
    val sem = new Semaphore(0)
    @volatile var ok = true
    for (i <- 0 until 10) {
      new Thread {
        override def run() {
          val answer1 = nums.reduce(_ + _)
          if (answer1 != 55) {
            printf("In thread %d: answer1 was %d\n", i, answer1)
            ok = false
          }
          val answer2 = nums.first()    // This will run "locally" in the current thread
          if (answer2 != 1) {
            printf("In thread %d: answer2 was %d\n", i, answer2)
            ok = false
          }
          sem.release()
        }
      }.start()
    }
    sem.acquire(10)
    if (!ok) {
      fail("One or more threads got the wrong answer from an RDD operation")
    }
  }

  test("accessing multi-threaded SparkContext form multiple threads") {
    sc = new SparkContext("local[4]", "test")
    val nums = sc.parallelize(1 to 10, 2)
    val sem = new Semaphore(0)
    @volatile var ok = true
    for (i <- 0 until 10) {
      new Thread {
        override def run() {
          val answer1 = nums.reduce(_ + _)
          if (answer1 != 55) {
            printf("In thread %d: answer1 was %d\n", i, answer1)
            ok = false
          }
          val answer2 = nums.first()    // This will run "locally" in the current thread
          if (answer2 != 1) {
            printf("In thread %d: answer2 was %d\n", i, answer2)
            ok = false
          }
          sem.release()
        }
      }.start()
    }
    sem.acquire(10)
    if (!ok) {
      fail("One or more threads got the wrong answer from an RDD operation")
    }
  }

  test("parallel job execution") {
    // This test launches two jobs with two threads each on a 4-core local cluster. Each thread
    // waits until there are 4 threads running at once, to test that both jobs have been launched.
    sc = new SparkContext("local[4]", "test")
    val nums = sc.parallelize(1 to 2, 2)
    val sem = new Semaphore(0)
    ThreadingSuiteState.clear()
    for (i <- 0 until 2) {
      new Thread {
        override def run() {
          val ans = nums.map(number => {
            val running = ThreadingSuiteState.runningThreads
            running.getAndIncrement()
            val time = System.currentTimeMillis()
            while (running.get() != 4 && System.currentTimeMillis() < time + 1000) {
              Thread.sleep(100)
            }
            if (running.get() != 4) {
              println("Waited 1 second without seeing runningThreads = 4 (it was " +
                running.get() + "); failing test")
              ThreadingSuiteState.failed.set(true)
            }
            number
          }).collect()
          assert(ans.toList === List(1, 2))
          sem.release()
        }
      }.start()
    }
    sem.acquire(2)
    if (ThreadingSuiteState.failed.get()) {
      fail("One or more threads didn't see runningThreads = 4")
    }
  }

  test("set local properties in different thread") {
    sc = new SparkContext("local", "test")
    val sem = new Semaphore(0)

    val threads = (1 to 5).map { i =>
      new Thread() {
        override def run() {
          sc.setLocalProperty("test", i.toString)
          assert(sc.getLocalProperty("test") === i.toString)
          sem.release()
        }
      }
    }

    threads.foreach(_.start())

    sem.acquire(5)
    assert(sc.getLocalProperty("test") === null)
  }

  test("set and get local properties in parent-children thread") {
    sc = new SparkContext("local", "test")
    sc.setLocalProperty("test", "parent")
    val sem = new Semaphore(0)

    val threads = (1 to 5).map { i =>
      new Thread() {
        override def run() {
          assert(sc.getLocalProperty("test") === "parent")
          sc.setLocalProperty("test", i.toString)
          assert(sc.getLocalProperty("test") === i.toString)
          sem.release()
        }
      }
    }

    threads.foreach(_.start())

    sem.acquire(5)
    assert(sc.getLocalProperty("test") === "parent")
    assert(sc.getLocalProperty("Foo") === null)
  }
}
