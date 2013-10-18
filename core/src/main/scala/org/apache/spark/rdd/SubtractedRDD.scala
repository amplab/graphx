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

package org.apache.spark.rdd

import java.util.{HashMap => JHashMap}
import scala.collection.JavaConversions._
import scala.collection.mutable.ArrayBuffer
import org.apache.spark.Partitioner
import org.apache.spark.Dependency
import org.apache.spark.TaskContext
import org.apache.spark.Partition
import org.apache.spark.SparkEnv
import org.apache.spark.ShuffleDependency
import org.apache.spark.OneToOneDependency


/**
 * An optimized version of cogroup for set difference/subtraction.
 *
 * It is possible to implement this operation with just `cogroup`, but
 * that is less efficient because all of the entries from `rdd2`, for
 * both matching and non-matching values in `rdd1`, are kept in the
 * JHashMap until the end.
 *
 * With this implementation, only the entries from `rdd1` are kept in-memory,
 * and the entries from `rdd2` are essentially streamed, as we only need to
 * touch each once to decide if the value needs to be removed.
 *
 * This is particularly helpful when `rdd1` is much smaller than `rdd2`, as
 * you can use `rdd1`'s partitioner/partition size and not worry about running
 * out of memory because of the size of `rdd2`.
 */
private[spark] class SubtractedRDD[K: ClassManifest, V: ClassManifest, W: ClassManifest](
    @transient var rdd1: RDD[_ <: Product2[K, V]],
    @transient var rdd2: RDD[_ <: Product2[K, W]],
    part: Partitioner)
  extends RDD[(K, V)](rdd1.context, Nil) {

  private var serializerClass: String = null

  def setSerializer(cls: String): SubtractedRDD[K, V, W] = {
    serializerClass = cls
    this
  }

  override def getDependencies: Seq[Dependency[_]] = {
    Seq(rdd1, rdd2).map { rdd =>
      if (rdd.partitioner == Some(part)) {
        logDebug("Adding one-to-one dependency with " + rdd)
        new OneToOneDependency(rdd)
      } else {
        logDebug("Adding shuffle dependency with " + rdd)
        new ShuffleDependency(rdd, part, serializerClass)
      }
    }
  }

  override def getPartitions: Array[Partition] = {
    val array = new Array[Partition](part.numPartitions)
    for (i <- 0 until array.size) {
      // Each CoGroupPartition will depend on rdd1 and rdd2
      array(i) = new CoGroupPartition(i, Seq(rdd1, rdd2).zipWithIndex.map { case (rdd, j) =>
        dependencies(j) match {
          case s: ShuffleDependency[_, _] =>
            new ShuffleCoGroupSplitDep(s.shuffleId)
          case _ =>
            new NarrowCoGroupSplitDep(rdd, i, rdd.partitions(i))
        }
      }.toArray)
    }
    array
  }

  override val partitioner = Some(part)

  override def compute(p: Partition, context: TaskContext): Iterator[(K, V)] = {
    val partition = p.asInstanceOf[CoGroupPartition]
    val serializer = SparkEnv.get.serializerManager.get(serializerClass)
    val map = new JHashMap[K, ArrayBuffer[V]]
    def getSeq(k: K): ArrayBuffer[V] = {
      val seq = map.get(k)
      if (seq != null) {
        seq
      } else {
        val seq = new ArrayBuffer[V]()
        map.put(k, seq)
        seq
      }
    }
    def integrate(dep: CoGroupSplitDep, op: Product2[K, V] => Unit) = dep match {
      case NarrowCoGroupSplitDep(rdd, _, itsSplit) => {
        rdd.iterator(itsSplit, context).asInstanceOf[Iterator[Product2[K, V]]].foreach(op)
      }
      case ShuffleCoGroupSplitDep(shuffleId) => {
        val iter = SparkEnv.get.shuffleFetcher.fetch[Product2[K, V]](shuffleId, partition.index,
          context, serializer)
        iter.foreach(op)
      }
    }
    // the first dep is rdd1; add all values to the map
    integrate(partition.deps(0), t => getSeq(t._1) += t._2)
    // the second dep is rdd2; remove all of its keys
    integrate(partition.deps(1), t => map.remove(t._1))
    map.iterator.map { t =>  t._2.iterator.map { (t._1, _) } }.flatten
  }

  override def clearDependencies() {
    super.clearDependencies()
    rdd1 = null
    rdd2 = null
  }

}
