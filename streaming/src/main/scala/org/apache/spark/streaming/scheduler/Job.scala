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

package org.apache.spark.streaming.scheduler

import org.apache.spark.streaming.Time

/**
 * Class representing a Spark computation. It may contain multiple Spark jobs.
 */
private[streaming]
class Job(val time: Time, func: () => _) {
  var id: String = _

  def run(): Long = {
    val startTime = System.currentTimeMillis 
    func() 
    val stopTime = System.currentTimeMillis
    (stopTime - startTime)
  }

  def setId(number: Int) {
    id = "streaming job " + time + "." + number
  }

  override def toString = id
}