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

package org.apache.spark.streaming.dstream

import org.apache.spark.rdd.RDD
import org.apache.spark.Partitioner
import org.apache.spark.SparkContext._
import org.apache.spark.storage.StorageLevel
import org.apache.spark.streaming.{Duration, Time, DStream}

import scala.reflect.ClassTag

private[streaming]
class StateDStream[K: ClassTag, V: ClassTag, S: ClassTag](
    parent: DStream[(K, V)],
    updateFunc: (Iterator[(K, Seq[V], Option[S])]) => Iterator[(K, S)],
    partitioner: Partitioner,
    preservePartitioning: Boolean
  ) extends DStream[(K, S)](parent.ssc) {

  super.persist(StorageLevel.MEMORY_ONLY_SER)

  override def dependencies = List(parent)

  override def slideDuration: Duration = parent.slideDuration

  override val mustCheckpoint = true

  override def compute(validTime: Time): Option[RDD[(K, S)]] = {

    // Try to get the previous state RDD
    getOrCompute(validTime - slideDuration) match {

      case Some(prevStateRDD) => {    // If previous state RDD exists

        // Try to get the parent RDD
        parent.getOrCompute(validTime) match {
          case Some(parentRDD) => {   // If parent RDD exists, then compute as usual

            // Define the function for the mapPartition operation on cogrouped RDD;
            // first map the cogrouped tuple to tuples of required type,
            // and then apply the update function
            val updateFuncLocal = updateFunc
            val finalFunc = (iterator: Iterator[(K, (Seq[V], Seq[S]))]) => {
              val i = iterator.map(t => {
                (t._1, t._2._1, t._2._2.headOption)
              })
              updateFuncLocal(i)
            }
            val cogroupedRDD = parentRDD.cogroup(prevStateRDD, partitioner)
            val stateRDD = cogroupedRDD.mapPartitions(finalFunc, preservePartitioning)
            //logDebug("Generating state RDD for time " + validTime)
            return Some(stateRDD)
          }
          case None => {    // If parent RDD does not exist

            // Re-apply the update function to the old state RDD
            val updateFuncLocal = updateFunc
            val finalFunc = (iterator: Iterator[(K, S)]) => {
              val i = iterator.map(t => (t._1, Seq[V](), Option(t._2)))
              updateFuncLocal(i)
            }
            val stateRDD = prevStateRDD.mapPartitions(finalFunc, preservePartitioning)
            return Some(stateRDD)
          }
        }
      }

      case None => {    // If previous session RDD does not exist (first input data)

        // Try to get the parent RDD
        parent.getOrCompute(validTime) match {
          case Some(parentRDD) => {   // If parent RDD exists, then compute as usual

            // Define the function for the mapPartition operation on grouped RDD;
            // first map the grouped tuple to tuples of required type,
            // and then apply the update function
            val updateFuncLocal = updateFunc
            val finalFunc = (iterator: Iterator[(K, Seq[V])]) => {
              updateFuncLocal(iterator.map(tuple => (tuple._1, tuple._2, None)))
            }

            val groupedRDD = parentRDD.groupByKey(partitioner)
            val sessionRDD = groupedRDD.mapPartitions(finalFunc, preservePartitioning)
            //logDebug("Generating state RDD for time " + validTime + " (first)")
            return Some(sessionRDD)
          }
          case None => { // If parent RDD does not exist, then nothing to do!
            //logDebug("Not generating state RDD (no previous state, no parent)")
            return None
          }
        }
      }
    }
  }
}
