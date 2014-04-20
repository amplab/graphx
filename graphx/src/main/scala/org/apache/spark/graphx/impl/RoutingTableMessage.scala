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

package org.apache.spark.graphx.impl

import scala.reflect.{classTag, ClassTag}

import org.apache.spark.Partitioner
import org.apache.spark.graphx.{PartitionID, VertexId}
import org.apache.spark.rdd.{ShuffledRDD, RDD}

/** See RoutingTablePartition.edgePartitionToMsgs */
private[graphx]
class RoutingTableMessage(
    var vid: VertexId,
    var pid: PartitionID,
    var position: Byte)
  extends Product2[VertexId, (PartitionID, Byte)] with Serializable {

  override def _1 = vid

  override def _2 = (pid, position)

  override def canEqual(that: Any): Boolean = that.isInstanceOf[RoutingTableMessage]
}

private[graphx]
class RoutingTableMessageRDDFunctions(self: RDD[RoutingTableMessage]) {
  def copartitionWithVertices(partitioner: Partitioner): RDD[RoutingTableMessage] = {
    new ShuffledRDD[VertexId, (PartitionID, Byte), RoutingTableMessage](self, partitioner)
      .setSerializer(new RoutingTableMessageSerializer)
  }
}

private[graphx]
object RoutingTableMessageRDDFunctions {
  implicit def rdd2RoutingTableMessageRDDFunctions(rdd: RDD[RoutingTableMessage]) = {
    new RoutingTableMessageRDDFunctions(rdd)
  }
}
