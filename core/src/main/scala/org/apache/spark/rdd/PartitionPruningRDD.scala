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

import scala.reflect.ClassTag

import org.apache.spark.{NarrowDependency, SparkEnv, Partition, TaskContext}


class PartitionPruningRDDPartition(idx: Int, val parentSplit: Partition) extends Partition {
  override val index = idx
}


/**
 * Represents a dependency between the PartitionPruningRDD and its parent. In this
 * case, the child RDD contains a subset of partitions of the parents'.
 */
class PruneDependency[T](rdd: RDD[T], @transient partitionFilterFunc: Int => Boolean)
  extends NarrowDependency[T](rdd) {

  @transient
  val partitions: Array[Partition] = rdd.partitions
    .filter(s => partitionFilterFunc(s.index)).zipWithIndex
    .map { case(split, idx) => new PartitionPruningRDDPartition(idx, split) : Partition }

  override def getParents(partitionId: Int) = {
    List(partitions(partitionId).asInstanceOf[PartitionPruningRDDPartition].parentSplit.index)
  }
}


/**
 * A RDD used to prune RDD partitions/partitions so we can avoid launching tasks on
 * all partitions. An example use case: If we know the RDD is partitioned by range,
 * and the execution DAG has a filter on the key, we can avoid launching tasks
 * on partitions that don't have the range covering the key.
 */
class PartitionPruningRDD[T: ClassTag](
    @transient prev: RDD[T],
    @transient partitionFilterFunc: Int => Boolean)
  extends RDD[T](prev.context, List(new PruneDependency(prev, partitionFilterFunc))) {

  override def compute(split: Partition, context: TaskContext) = firstParent[T].iterator(
    split.asInstanceOf[PartitionPruningRDDPartition].parentSplit, context)

  override protected def getPartitions: Array[Partition] =
    getDependencies.head.asInstanceOf[PruneDependency[T]].partitions
}


object PartitionPruningRDD {

  /**
   * Create a PartitionPruningRDD. This function can be used to create the PartitionPruningRDD
   * when its type T is not known at compile time.
   */
  def create[T](rdd: RDD[T], partitionFilterFunc: Int => Boolean) = {
    new PartitionPruningRDD[T](rdd, partitionFilterFunc)(rdd.elementClassTag)
  }
}
