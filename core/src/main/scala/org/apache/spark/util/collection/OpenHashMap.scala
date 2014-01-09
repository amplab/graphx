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

package org.apache.spark.util.collection

import scala.reflect.ClassTag

/**
 * A fast hash map implementation for nullable keys. This hash map supports insertions and updates,
 * but not deletions. This map is about 5X faster than java.util.HashMap, while using much less
 * space overhead.
 *
 * Under the hood, it uses our OpenHashSet implementation.
 */
private[spark]
class OpenHashMap[K >: Null : ClassTag, @specialized(Long, Int, Double) V: ClassTag](
  val keySet: OpenHashSet[K], var _values: Array[V])
  extends Iterable[(K, V)]
  with Serializable {

  /**
   * Allocate an OpenHashMap with a fixed initial capacity
   */
  def this(initialCapacity: Int = 64) =
    this(new OpenHashSet[K](initialCapacity), new Array[V](initialCapacity))

  /**
   * Allocate an OpenHashMap with a fixed initial capacity
   */
  def this(keySet: OpenHashSet[K]) = this(keySet, new Array[V](keySet.capacity))

  @transient private var _oldValues: Array[V] = null

  // Treat the null key differently so we can use nulls in "data" to represent empty items.
  private var haveNullValue = false
  private var nullValue: V = null.asInstanceOf[V]

  override def size: Int = if (haveNullValue) keySet.size + 1 else keySet.size

  /** Get the value for a given key */
  def apply(k: K): V = {
    if (k == null) {
      nullValue
    } else {
      val pos = keySet.getPos(k)
      if (pos < 0) {
        null.asInstanceOf[V]
      } else {
        _values(pos)
      }
    }
  }

  /** Set the value for a key */
  def update(k: K, v: V) {
    if (k == null) {
      haveNullValue = true
      nullValue = v
    } else {
      val pos = keySet.addWithoutResize(k) & OpenHashSet.POSITION_MASK
      _values(pos) = v
      keySet.rehashIfNeeded(k, grow, move)
      _oldValues = null
    }
  }

  /** Set the value for a key */
  def setMerge(k: K, v: V, mergeF: (V,V) => V) {
    if (k == null) {
      if(haveNullValue) {
        nullValue = mergeF(nullValue, v)
      } else {
        haveNullValue = true
        nullValue = v
      }
    } else {
      val pos = keySet.addWithoutResize(k) & OpenHashSet.POSITION_MASK
      _values(pos) = mergeF(_values(pos), v)
      keySet.rehashIfNeeded(k, grow, move)
      _oldValues = null
    }
  }

  /**
   * If the key doesn't exist yet in the hash map, set its value to defaultValue; otherwise,
   * set its value to mergeValue(oldValue).
   *
   * @return the newly updated value.
   */
  def changeValue(k: K, defaultValue: => V, mergeValue: (V) => V): V = {
    if (k == null) {
      if (haveNullValue) {
        nullValue = mergeValue(nullValue)
      } else {
        haveNullValue = true
        nullValue = defaultValue
      }
      nullValue
    } else {
      val pos = keySet.addWithoutResize(k)
      if ((pos & OpenHashSet.NONEXISTENCE_MASK) != 0) {
        val newValue = defaultValue
        _values(pos & OpenHashSet.POSITION_MASK) = newValue
        keySet.rehashIfNeeded(k, grow, move)
        newValue
      } else {
        _values(pos) = mergeValue(_values(pos))
        _values(pos)
      }
    }
  }

  override def iterator = new Iterator[(K, V)] {
    var pos = -1
    var nextPair: (K, V) = computeNextPair()

    /** Get the next value we should return from next(), or null if we're finished iterating */
    def computeNextPair(): (K, V) = {
      if (pos == -1) {    // Treat position -1 as looking at the null value
        if (haveNullValue) {
          pos += 1
          return (null.asInstanceOf[K], nullValue)
        }
        pos += 1
      }
      pos = keySet.nextPos(pos)
      if (pos >= 0) {
        val ret = (keySet.getValue(pos), _values(pos))
        pos += 1
        ret
      } else {
        null
      }
    }

    def hasNext = nextPair != null

    def next() = {
      val pair = nextPair
      nextPair = computeNextPair()
      pair
    }
  }

  // The following member variables are declared as protected instead of private for the
  // specialization to work (specialized class extends the non-specialized one and needs access
  // to the "private" variables).
  // They also should have been val's. We use var's because there is a Scala compiler bug that
  // would throw illegal access error at runtime if they are declared as val's.
  protected var grow = (newCapacity: Int) => {
    _oldValues = _values
    _values = new Array[V](newCapacity)
  }

  protected var move = (oldPos: Int, newPos: Int) => {
    _values(newPos) = _oldValues(oldPos)
  }
}
