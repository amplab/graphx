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

package org.apache.spark.examples

import org.apache.spark.SparkContext
import org.apache.spark.SparkContext._
import java.util.Random

object GroupByTest {
  def main(args: Array[String]) {
    if (args.length == 0) {
      System.err.println("Usage: GroupByTest <master> [numMappers] [numKVPairs] [KeySize] [numReducers]")
      System.exit(1)
    }
    
    var numMappers = if (args.length > 1) args(1).toInt else 2
    var numKVPairs = if (args.length > 2) args(2).toInt else 1000
    var valSize = if (args.length > 3) args(3).toInt else 1000
    var numReducers = if (args.length > 4) args(4).toInt else numMappers

    val sc = new SparkContext(args(0), "GroupBy Test",
      System.getenv("SPARK_HOME"), SparkContext.jarOfClass(this.getClass))
    
    val pairs1 = sc.parallelize(0 until numMappers, numMappers).flatMap { p =>
      val ranGen = new Random
      var arr1 = new Array[(Int, Array[Byte])](numKVPairs)
      for (i <- 0 until numKVPairs) {
        val byteArr = new Array[Byte](valSize)
        ranGen.nextBytes(byteArr)
        arr1(i) = (ranGen.nextInt(Int.MaxValue), byteArr)
      }
      arr1
    }.cache
    // Enforce that everything has been calculated and in cache
    pairs1.count
    
    println(pairs1.groupByKey(numReducers).count)

    System.exit(0)
  }
}

