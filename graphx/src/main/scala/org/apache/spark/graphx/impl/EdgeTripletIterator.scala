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

import org.apache.spark.graphx._
import org.apache.spark.graphx.util.collection.PrimitiveKeyOpenHashMap

/**
 * The Iterator type returned when constructing edge triplets. This class technically could be
 * an anonymous class in GraphImpl.triplets, but we name it here explicitly so it is easier to
 * debug / profile.
 */
private[impl]
class EdgeTripletIterator[VD: ClassTag, ED: ClassTag](
    val vidToIndex: VertexIdToIndexMap,
    val vertexArray: Array[VD],
    val edgePartition: EdgePartition[ED])
  extends Iterator[EdgeTriplet[VD, ED]] {

  private var ePartIter: Iterator[Edge[ED]] = edgePartition.iterator

  // A triplet object that this iterator.next() call returns. We reuse this object to avoid
  // allocating too many temporary Java objects.
  private val triplet = new EdgeTriplet[VD, ED]

  override def hasNext: Boolean = ePartIter.hasNext

  override def next() = {
    val edge = ePartIter.next()
    triplet.set(edge)
    triplet.srcAttr = vertexArray(edge.srcLocalVid)
    triplet.dstAttr = vertexArray(edge.dstLocalVid)
    triplet
  }
}
