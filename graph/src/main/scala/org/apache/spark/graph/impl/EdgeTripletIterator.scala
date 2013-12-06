package org.apache.spark.graph.impl

import org.apache.spark.graph._
import org.apache.spark.util.collection.PrimitiveKeyOpenHashMap


/**
 * The Iterator type returned when constructing edge triplets. This class technically could be
 * an anonymous class in GraphImpl.triplets, but we name it here explicitly so it is easier to
 * debug / profile.
 */
private[impl]
class EdgeTripletIterator[VD: ClassManifest, ED: ClassManifest](
    val edgePartition: EdgePartition[ED],
    val vmap: PrimitiveKeyOpenHashMap[Vid, VD])
  extends Iterator[EdgeTriplet[VD, ED]] {

  // Current position in the array.
  private var pos = 0

  // A triplet object that this iterator.next() call returns. We reuse this object to avoid
  // allocating too many temporary Java objects.
  private val triplet = new EdgeTriplet[VD, ED]

  override def hasNext: Boolean = pos < edgePartition.size

  override def next() = {
    triplet.srcId = edgePartition.srcIds(pos)
    // assert(vmap.containsKey(e.src.id))
    triplet.srcAttr = vmap(triplet.srcId)
    triplet.dstId = edgePartition.dstIds(pos)
    // assert(vmap.containsKey(e.dst.id))
    triplet.dstAttr = vmap(triplet.dstId)
    triplet.attr = edgePartition.data(pos)
    pos += 1
    triplet
  }
}
