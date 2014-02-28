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

import scala.reflect.ClassTag
import scala.util.Random

import org.scalatest.FunSuite

import org.apache.spark.graphx._

class EdgePartitionSuite extends FunSuite {

  test("reverse") {
    val edges = Set(Edge(0, 1, 0), Edge(1, 2, 0), Edge(2, 0, 0))
    val reversedEdges = Set(Edge(0, 2, 0), Edge(1, 0, 0), Edge(2, 1, 0))
    val builder = new FreshEdgePartitionBuilder[Int]
    for (e <- edges) {
      builder.add(e.srcId, e.dstId, e.attr)
    }
    val edgePartition = builder.toEdgePartition
    assert(edgePartition.reverse.iterator.map(_.copy()).toSet === reversedEdges)
    assert(edgePartition.reverse.reverse.iterator.map(_.copy()).toSet === edges)
  }

  test("map") {
    val edges = Set(Edge(0, 1, 0), Edge(1, 2, 0), Edge(2, 0, 0))
    val builder = new FreshEdgePartitionBuilder[Int]
    for (e <- edges) {
      builder.add(e.srcId, e.dstId, e.attr)
    }
    val edgePartition = builder.toEdgePartition
    assert(edgePartition.map(e => e.srcId + e.dstId).iterator.map(_.copy()).toSet ===
      edges.map(e => e.copy(attr = e.srcId + e.dstId)))
  }

  test("groupEdges") {
    val edges = Set(
      Edge(0, 1, 1), Edge(1, 2, 2), Edge(2, 0, 4), Edge(0, 1, 8), Edge(1, 2, 16), Edge(2, 0, 32))
    val groupedEdges = Set(Edge(0, 1, 9), Edge(1, 2, 18), Edge(2, 0, 36))
    val builder = new FreshEdgePartitionBuilder[Int]
    for (e <- edges) {
      builder.add(e.srcId, e.dstId, e.attr)
    }
    val edgePartition = builder.toEdgePartition
    assert(edgePartition.groupEdges(_ + _).iterator.map(_.copy()).toSet === groupedEdges)
  }

  test("indexIterator") {
    val edgesFrom0 = Set(Edge(0, 1, 0))
    val edgesFrom1 = Set(Edge(1, 0, 0), Edge(1, 2, 0))
    val sortedEdges = edgesFrom0 ++ edgesFrom1
    val builder = new FreshEdgePartitionBuilder[Int]
    for (e <- Random.shuffle(sortedEdges)) {
      builder.add(e.srcId, e.dstId, e.attr)
    }

    val edgePartition = builder.toEdgePartition
    assert(edgePartition.iterator.map(_.copy()).toSet === sortedEdges)
    assert(edgePartition.indexIterator(edgePartition.lookup(_) == 0).map(_.copy()).toSet ===
      edgesFrom0)
    assert(edgePartition.indexIterator(edgePartition.lookup(_) == 1).map(_.copy()).toSet ===
      edgesFrom1)
  }

  test("innerJoin") {
    def makeEdgePartition[A: ClassTag](xs: Iterable[(Int, Int, A)]): EdgePartition[A] = {
      val builder = new FreshEdgePartitionBuilder[A]
      for ((src, dst, attr) <- xs) { builder.add(src: VertexId, dst: VertexId, attr) }
      builder.toEdgePartition
    }
    val aSet = Set((0, 1, 0), (1, 0, 0), (1, 2, 0), (5, 4, 0), (5, 5, 0))
    val bSet = Set((0, 1, 0), (1, 0, 0), (1, 1, 0), (3, 4, 0), (5, 5, 0))
    val a = makeEdgePartition(aSet)
    val b = makeEdgePartition(bSet)

    assert(a.innerJoin(b) { (src, dst, a, b) => a }.iterator.map(_.copy()).toSet ===
      Set(Edge(0, 1, 0), Edge(1, 0, 0), Edge(5, 5, 0)))
  }

  test("vertexIds") {
    def makeEdgePartition(xs: Iterable[(Long, Long)]): EdgePartition[Boolean] = {
      val builder = new FreshEdgePartitionBuilder[Boolean]
      for ((src, dst) <- xs) { builder.add(src: VertexId, dst: VertexId, false) }
      builder.toEdgePartition
    }
    val aSet = (0L to 98L).zip((1L to 99L) :+ 0L)
    val a = makeEdgePartition(aSet)
    val vids = a.vertexIds.toSet
    assert(vids.toList.sorted === (0L to 99L).toList)
  }
}
