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

import org.apache.spark.Logging
import org.apache.spark.SparkContext._
import org.apache.spark.rdd.RDD
import org.apache.spark.util.collection.{PrimitiveVector, OpenHashSet}

import org.apache.spark.graphx._

/**
 * A view of the vertices after they are shipped to the join sites specified in
 * `vertexPlacement`. The resulting view is co-partitioned with `edges`. If `prevViewOpt` is
 * specified, `updatedVerts` are treated as incremental updates to the previous view. Otherwise, a
 * fresh view is created.
 *
 * The view is always cached (i.e., once it is evaluated, it remains materialized). This avoids
 * constructing it twice if the user calls graph.triplets followed by graph.mapReduceTriplets, for
 * example. However, it means iterative algorithms must manually call `Graph.unpersist` on previous
 * iterations' graphs for best GC performance. See the implementation of
 * [[org.apache.spark.graphx.Pregel]] for an example.
 */
private[impl]
class ReplicatedVertexView[VD: ClassTag, ED: ClassTag](
    var edges: EdgeRDD[ED, VD],
    var hasSrcId: Boolean = false,
    var hasDstId: Boolean = false) extends Logging {

  def withEdges[VD2: ClassTag, ED2: ClassTag](
      edges_ : EdgeRDD[ED2, VD2]): ReplicatedVertexView[VD2, ED2] = {
    new ReplicatedVertexView(edges_, hasSrcId, hasDstId)
  }

  def upgrade(vertices: VertexRDD[VD], includeSrc: Boolean, includeDst: Boolean) {
    val shipSrc = includeSrc && !hasSrcId
    val shipDst = includeDst && !hasDstId
    if (shipSrc || shipDst) {
      val shippedVerts: RDD[(Int, VertexAttributeBlock[VD])] =
        vertices.shipVertexAttributes(shipSrc, shipDst)
          .setName("ReplicatedVertexView.upgrade(%s, %s) - shippedVerts %s %s (broadcast)".format(
            includeSrc, includeDst, shipSrc, shipDst))
          .partitionBy(edges.partitioner.get)
      val newEdges = new EdgeRDD(edges.partitionsRDD.zipPartitions(shippedVerts) {
        (ePartIter, shippedVertsIter) => ePartIter.map {
          case (pid, edgePartition) =>
            (pid, edgePartition.updateVertices(shippedVertsIter.flatMap(_._2.iterator)))
        }
      })
      edges = newEdges
      hasSrcId = includeSrc
      hasDstId = includeDst
    }
  }

  def withActiveSet(actives: VertexRDD[_]): ReplicatedVertexView[VD, ED] = {
    // Ship active sets to edge partitions using vertexPlacement, but ignoring includeSrc and
    // includeDst. These flags govern attribute shipping, but the activeness of a vertex must be
    // shipped to all edges mentioning that vertex, regardless of whether the vertex attribute is
    // also shipped there.
    val shippedActives = actives.shipVertexIds()
      .setName("ReplicatedVertexView.withActiveSet - shippedActives (broadcast)")
      .partitionBy(edges.partitioner.get)

    // Update the view with shippedActives, setting activeness flags in the resulting
    // VertexPartitions
    val newEdges = new EdgeRDD(edges.partitionsRDD.zipPartitions(shippedActives) {
      (ePartIter, shippedActivesIter) => ePartIter.map {
        case (pid, edgePartition) =>
          (pid, edgePartition.withActiveSet(shippedActivesIter.flatMap(_._2.iterator)))
      }
    })

    new ReplicatedVertexView(newEdges, hasSrcId, hasDstId)
  }

  def updateVertices(updates: VertexRDD[VD]): ReplicatedVertexView[VD, ED] = {
    // Ship vertex attributes to edge partitions according to vertexPlacement
    val shippedVerts = updates.shipVertexAttributes(hasSrcId, hasDstId)
      .setName("ReplicatedVertexView.updateVertices - shippedVerts %s %s (broadcast)".format(
        hasSrcId, hasDstId))
      .partitionBy(edges.partitioner.get)

    // Update prevView with shippedVerts, setting staleness flags in the resulting
    // VertexPartitions
    val newEdges = new EdgeRDD(edges.partitionsRDD.zipPartitions(shippedVerts) {
      (ePartIter, shippedVertsIter) => ePartIter.map {
        case (pid, edgePartition) =>
          (pid, edgePartition.updateVertices(shippedVertsIter.flatMap(_._2.iterator)))
      }
    })

    new ReplicatedVertexView(newEdges, hasSrcId, hasDstId)
  }
}
