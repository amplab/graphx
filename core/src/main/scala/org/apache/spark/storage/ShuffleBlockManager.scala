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

package org.apache.spark.storage

import java.io.File
import java.util.concurrent.ConcurrentLinkedQueue
import java.util.concurrent.atomic.AtomicInteger

import scala.collection.JavaConversions._

import org.apache.spark.Logging
import org.apache.spark.serializer.Serializer
import org.apache.spark.storage.ShuffleBlockManager.ShuffleFileGroup
import org.apache.spark.util.{MetadataCleaner, MetadataCleanerType, TimeStampedHashMap}
import org.apache.spark.util.collection.{PrimitiveKeyOpenHashMap, PrimitiveVector}

/** A group of writers for a ShuffleMapTask, one writer per reducer. */
private[spark] trait ShuffleWriterGroup {
  val writers: Array[BlockObjectWriter]

  /** @param success Indicates all writes were successful. If false, no blocks will be recorded. */
  def releaseWriters(success: Boolean)
}

/**
 * Manages assigning disk-based block writers to shuffle tasks. Each shuffle task gets one file
 * per reducer (this set of files is called a ShuffleFileGroup).
 *
 * As an optimization to reduce the number of physical shuffle files produced, multiple shuffle
 * blocks are aggregated into the same file. There is one "combined shuffle file" per reducer
 * per concurrently executing shuffle task. As soon as a task finishes writing to its shuffle
 * files, it releases them for another task.
 * Regarding the implementation of this feature, shuffle files are identified by a 3-tuple:
 *   - shuffleId: The unique id given to the entire shuffle stage.
 *   - bucketId: The id of the output partition (i.e., reducer id)
 *   - fileId: The unique id identifying a group of "combined shuffle files." Only one task at a
 *       time owns a particular fileId, and this id is returned to a pool when the task finishes.
 * Each shuffle file is then mapped to a FileSegment, which is a 3-tuple (file, offset, length)
 * that specifies where in a given file the actual block data is located.
 *
 * Shuffle file metadata is stored in a space-efficient manner. Rather than simply mapping
 * ShuffleBlockIds directly to FileSegments, each ShuffleFileGroup maintains a list of offsets for
 * each block stored in each file. In order to find the location of a shuffle block, we search the
 * files within a ShuffleFileGroups associated with the block's reducer.
 */
private[spark]
class ShuffleBlockManager(blockManager: BlockManager) extends Logging {
  def conf = blockManager.conf

  // Turning off shuffle file consolidation causes all shuffle Blocks to get their own file.
  // TODO: Remove this once the shuffle file consolidation feature is stable.
  val consolidateShuffleFiles =
    conf.getBoolean("spark.shuffle.consolidateFiles", false)

  private val bufferSize = conf.getInt("spark.shuffle.file.buffer.kb", 100) * 1024

  /**
   * Contains all the state related to a particular shuffle. This includes a pool of unused
   * ShuffleFileGroups, as well as all ShuffleFileGroups that have been created for the shuffle.
   */
  private class ShuffleState(val numBuckets: Int) {
    val nextFileId = new AtomicInteger(0)
    val unusedFileGroups = new ConcurrentLinkedQueue[ShuffleFileGroup]()
    val allFileGroups = new ConcurrentLinkedQueue[ShuffleFileGroup]()

    /**
     * The mapIds of all map tasks completed on this Executor for this shuffle.
     * NB: This is only populated if consolidateShuffleFiles is FALSE. We don't need it otherwise.
     */
    val completedMapTasks = new ConcurrentLinkedQueue[Int]()
  }

  type ShuffleId = Int
  private val shuffleStates = new TimeStampedHashMap[ShuffleId, ShuffleState]

  private val metadataCleaner =
    new MetadataCleaner(MetadataCleanerType.SHUFFLE_BLOCK_MANAGER, this.cleanup, conf)

  def forMapTask(shuffleId: Int, mapId: Int, numBuckets: Int, serializer: Serializer) = {
    new ShuffleWriterGroup {
      shuffleStates.putIfAbsent(shuffleId, new ShuffleState(numBuckets))
      private val shuffleState = shuffleStates(shuffleId)
      private var fileGroup: ShuffleFileGroup = null

      val writers: Array[BlockObjectWriter] = if (consolidateShuffleFiles) {
        fileGroup = getUnusedFileGroup()
        Array.tabulate[BlockObjectWriter](numBuckets) { bucketId =>
          val blockId = ShuffleBlockId(shuffleId, mapId, bucketId)
          blockManager.getDiskWriter(blockId, fileGroup(bucketId), serializer, bufferSize)
        }
      } else {
        Array.tabulate[BlockObjectWriter](numBuckets) { bucketId =>
          val blockId = ShuffleBlockId(shuffleId, mapId, bucketId)
          val blockFile = blockManager.diskBlockManager.getFile(blockId)
          // Because of previous failures, the shuffle file may already exist on this machine.
          // If so, remove it.
          if (blockFile.exists) {
            if (blockFile.delete()) {
              logInfo(s"Removed existing shuffle file $blockFile")
            } else {
              logWarning(s"Failed to remove existing shuffle file $blockFile")
            }
          }
          blockManager.getDiskWriter(blockId, blockFile, serializer, bufferSize)
        }
      }

      override def releaseWriters(success: Boolean) {
        if (consolidateShuffleFiles) {
          if (success) {
            val offsets = writers.map(_.fileSegment().offset)
            fileGroup.recordMapOutput(mapId, offsets)
          }
          recycleFileGroup(fileGroup)
        } else {
          shuffleState.completedMapTasks.add(mapId)
        }
      }

      private def getUnusedFileGroup(): ShuffleFileGroup = {
        val fileGroup = shuffleState.unusedFileGroups.poll()
        if (fileGroup != null) fileGroup else newFileGroup()
      }

      private def newFileGroup(): ShuffleFileGroup = {
        val fileId = shuffleState.nextFileId.getAndIncrement()
        val files = Array.tabulate[File](numBuckets) { bucketId =>
          val filename = physicalFileName(shuffleId, bucketId, fileId)
          blockManager.diskBlockManager.getFile(filename)
        }
        val fileGroup = new ShuffleFileGroup(fileId, shuffleId, files)
        shuffleState.allFileGroups.add(fileGroup)
        fileGroup
      }

      private def recycleFileGroup(group: ShuffleFileGroup) {
        shuffleState.unusedFileGroups.add(group)
      }
    }
  }

  /**
   * Returns the physical file segment in which the given BlockId is located.
   * This function should only be called if shuffle file consolidation is enabled, as it is
   * an error condition if we don't find the expected block.
   */
  def getBlockLocation(id: ShuffleBlockId): FileSegment = {
    // Search all file groups associated with this shuffle.
    val shuffleState = shuffleStates(id.shuffleId)
    for (fileGroup <- shuffleState.allFileGroups) {
      val segment = fileGroup.getFileSegmentFor(id.mapId, id.reduceId)
      if (segment.isDefined) { return segment.get }
    }
    throw new IllegalStateException("Failed to find shuffle block: " + id)
  }

  private def physicalFileName(shuffleId: Int, bucketId: Int, fileId: Int) = {
    "merged_shuffle_%d_%d_%d".format(shuffleId, bucketId, fileId)
  }

  private def cleanup(cleanupTime: Long) {
    shuffleStates.clearOldValues(cleanupTime, (shuffleId, state) => {
      if (consolidateShuffleFiles) {
        for (fileGroup <- state.allFileGroups; file <- fileGroup.files) {
          file.delete()
        }
      } else {
        for (mapId <- state.completedMapTasks; reduceId <- 0 until state.numBuckets) {
          val blockId = new ShuffleBlockId(shuffleId, mapId, reduceId)
          blockManager.diskBlockManager.getFile(blockId).delete()
        }
      }
    })
  }

  def removeAllShuffleStuff() {
    for (state <- shuffleStates.values;
         group <- state.allFileGroups;
         (mapId, _) <- group.mapIdToIndex.iterator;
         reducerId <- 0 until group.files.length) {
      val blockId = new ShuffleBlockId(group.shuffleId, mapId, reducerId)
      blockManager.removeBlock(blockId, tellMaster = false)
    }
    shuffleStates.clear()
  }
}

private[spark]
object ShuffleBlockManager {
  /**
   * A group of shuffle files, one per reducer.
   * A particular mapper will be assigned a single ShuffleFileGroup to write its output to.
   */
  private class ShuffleFileGroup(val shuffleId: Int, val fileId: Int, val files: Array[File]) {
    /**
     * Stores the absolute index of each mapId in the files of this group. For instance,
     * if mapId 5 is the first block in each file, mapIdToIndex(5) = 0.
     */
    val mapIdToIndex = new PrimitiveKeyOpenHashMap[Int, Int]()

    /**
     * Stores consecutive offsets of blocks into each reducer file, ordered by position in the file.
     * This ordering allows us to compute block lengths by examining the following block offset.
     * Note: mapIdToIndex(mapId) returns the index of the mapper into the vector for every
     * reducer.
     */
    private val blockOffsetsByReducer = Array.fill[PrimitiveVector[Long]](files.length) {
      new PrimitiveVector[Long]()
    }

    def numBlocks = mapIdToIndex.size

    def apply(bucketId: Int) = files(bucketId)

    def recordMapOutput(mapId: Int, offsets: Array[Long]) {
      mapIdToIndex(mapId) = numBlocks
      for (i <- 0 until offsets.length) {
        blockOffsetsByReducer(i) += offsets(i)
      }
    }

    /** Returns the FileSegment associated with the given map task, or None if no entry exists. */
    def getFileSegmentFor(mapId: Int, reducerId: Int): Option[FileSegment] = {
      val file = files(reducerId)
      val blockOffsets = blockOffsetsByReducer(reducerId)
      val index = mapIdToIndex.getOrElse(mapId, -1)
      if (index >= 0) {
        val offset = blockOffsets(index)
        val length =
          if (index + 1 < numBlocks) {
            blockOffsets(index + 1) - offset
          } else {
            file.length() - offset
          }
        assert(length >= 0)
        Some(new FileSegment(file, offset, length))
      } else {
        None
      }
    }
  }
}
