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

import java.util.{HashMap => JHashMap}

import scala.collection.mutable
import scala.collection.JavaConversions._

import akka.actor.{Actor, ActorRef, Cancellable}
import akka.dispatch.Future
import akka.pattern.ask
import akka.util.Duration
import akka.util.duration._

import org.apache.spark.{Logging, SparkException}
import org.apache.spark.storage.BlockManagerMessages._
import org.apache.spark.util.Utils


/**
 * BlockManagerMasterActor is an actor on the master node to track statuses of
 * all slaves' block managers.
 */
private[spark]
class BlockManagerMasterActor(val isLocal: Boolean) extends Actor with Logging {

  // Mapping from block manager id to the block manager's information.
  private val blockManagerInfo =
    new mutable.HashMap[BlockManagerId, BlockManagerMasterActor.BlockManagerInfo]

  // Mapping from executor ID to block manager ID.
  private val blockManagerIdByExecutor = new mutable.HashMap[String, BlockManagerId]

  // Mapping from block id to the set of block managers that have the block.
  private val blockLocations = new JHashMap[BlockId, mutable.HashSet[BlockManagerId]]

  val akkaTimeout = Duration.create(
    System.getProperty("spark.akka.askTimeout", "10").toLong, "seconds")

  initLogging()

  val slaveTimeout = System.getProperty("spark.storage.blockManagerSlaveTimeoutMs",
    "" + (BlockManager.getHeartBeatFrequencyFromSystemProperties * 3)).toLong

  val checkTimeoutInterval = System.getProperty("spark.storage.blockManagerTimeoutIntervalMs",
    "60000").toLong

  var timeoutCheckingTask: Cancellable = null

  override def preStart() {
    if (!BlockManager.getDisableHeartBeatsForTesting) {
      timeoutCheckingTask = context.system.scheduler.schedule(
        0.seconds, checkTimeoutInterval.milliseconds, self, ExpireDeadHosts)
    }
    super.preStart()
  }

  def receive = {
    case RegisterBlockManager(blockManagerId, maxMemSize, slaveActor) =>
      register(blockManagerId, maxMemSize, slaveActor)
      sender ! true

    case UpdateBlockInfo(blockManagerId, blockId, storageLevel, deserializedSize, size) =>
      // TODO: Ideally we want to handle all the message replies in receive instead of in the
      // individual private methods.
      updateBlockInfo(blockManagerId, blockId, storageLevel, deserializedSize, size)

    case GetLocations(blockId) =>
      sender ! getLocations(blockId)

    case GetLocationsMultipleBlockIds(blockIds) =>
      sender ! getLocationsMultipleBlockIds(blockIds)

    case GetPeers(blockManagerId, size) =>
      sender ! getPeers(blockManagerId, size)

    case GetMemoryStatus =>
      sender ! memoryStatus

    case GetStorageStatus =>
      sender ! storageStatus

    case RemoveRdd(rddId) =>
      sender ! removeRdd(rddId)

    case RemoveBlock(blockId) =>
      removeBlockFromWorkers(blockId)
      sender ! true

    case RemoveExecutor(execId) =>
      removeExecutor(execId)
      sender ! true

    case StopBlockManagerMaster =>
      logInfo("Stopping BlockManagerMaster")
      sender ! true
      if (timeoutCheckingTask != null) {
        timeoutCheckingTask.cancel()
      }
      context.stop(self)

    case ExpireDeadHosts =>
      expireDeadHosts()

    case HeartBeat(blockManagerId) =>
      sender ! heartBeat(blockManagerId)

    case other =>
      logWarning("Got unknown message: " + other)
  }

  private def removeRdd(rddId: Int): Future[Seq[Int]] = {
    // First remove the metadata for the given RDD, and then asynchronously remove the blocks
    // from the slaves.

    // Find all blocks for the given RDD, remove the block from both blockLocations and
    // the blockManagerInfo that is tracking the blocks.
    val blocks = blockLocations.keys.flatMap(_.asRDDId).filter(_.rddId == rddId)
    blocks.foreach { blockId =>
      val bms: mutable.HashSet[BlockManagerId] = blockLocations.get(blockId)
      bms.foreach(bm => blockManagerInfo.get(bm).foreach(_.removeBlock(blockId)))
      blockLocations.remove(blockId)
    }

    // Ask the slaves to remove the RDD, and put the result in a sequence of Futures.
    // The dispatcher is used as an implicit argument into the Future sequence construction.
    import context.dispatcher
    val removeMsg = RemoveRdd(rddId)
    Future.sequence(blockManagerInfo.values.map { bm =>
      bm.slaveActor.ask(removeMsg)(akkaTimeout).mapTo[Int]
    }.toSeq)
  }

  private def removeBlockManager(blockManagerId: BlockManagerId) {
    val info = blockManagerInfo(blockManagerId)

    // Remove the block manager from blockManagerIdByExecutor.
    blockManagerIdByExecutor -= blockManagerId.executorId

    // Remove it from blockManagerInfo and remove all the blocks.
    blockManagerInfo.remove(blockManagerId)
    val iterator = info.blocks.keySet.iterator
    while (iterator.hasNext) {
      val blockId = iterator.next
      val locations = blockLocations.get(blockId)
      locations -= blockManagerId
      if (locations.size == 0) {
        blockLocations.remove(locations)
      }
    }
  }

  private def expireDeadHosts() {
    logTrace("Checking for hosts with no recent heart beats in BlockManagerMaster.")
    val now = System.currentTimeMillis()
    val minSeenTime = now - slaveTimeout
    val toRemove = new mutable.HashSet[BlockManagerId]
    for (info <- blockManagerInfo.values) {
      if (info.lastSeenMs < minSeenTime) {
        logWarning("Removing BlockManager " + info.blockManagerId + " with no recent heart beats: " +
          (now - info.lastSeenMs) + "ms exceeds " + slaveTimeout + "ms")
        toRemove += info.blockManagerId
      }
    }
    toRemove.foreach(removeBlockManager)
  }

  private def removeExecutor(execId: String) {
    logInfo("Trying to remove executor " + execId + " from BlockManagerMaster.")
    blockManagerIdByExecutor.get(execId).foreach(removeBlockManager)
  }

  private def heartBeat(blockManagerId: BlockManagerId): Boolean = {
    if (!blockManagerInfo.contains(blockManagerId)) {
      blockManagerId.executorId == "<driver>" && !isLocal
    } else {
      blockManagerInfo(blockManagerId).updateLastSeenMs()
      true
    }
  }

  // Remove a block from the slaves that have it. This can only be used to remove
  // blocks that the master knows about.
  private def removeBlockFromWorkers(blockId: BlockId) {
    val locations = blockLocations.get(blockId)
    if (locations != null) {
      locations.foreach { blockManagerId: BlockManagerId =>
        val blockManager = blockManagerInfo.get(blockManagerId)
        if (blockManager.isDefined) {
          // Remove the block from the slave's BlockManager.
          // Doesn't actually wait for a confirmation and the message might get lost.
          // If message loss becomes frequent, we should add retry logic here.
          blockManager.get.slaveActor ! RemoveBlock(blockId)
        }
      }
    }
  }

  // Return a map from the block manager id to max memory and remaining memory.
  private def memoryStatus: Map[BlockManagerId, (Long, Long)] = {
    blockManagerInfo.map { case(blockManagerId, info) =>
      (blockManagerId, (info.maxMem, info.remainingMem))
    }.toMap
  }

  private def storageStatus: Array[StorageStatus] = {
    blockManagerInfo.map { case(blockManagerId, info) =>
      import collection.JavaConverters._
      StorageStatus(blockManagerId, info.maxMem, info.blocks.asScala.toMap)
    }.toArray
  }

  private def register(id: BlockManagerId, maxMemSize: Long, slaveActor: ActorRef) {
    if (id.executorId == "<driver>" && !isLocal) {
      // Got a register message from the master node; don't register it
    } else if (!blockManagerInfo.contains(id)) {
      blockManagerIdByExecutor.get(id.executorId) match {
        case Some(manager) =>
          // A block manager of the same executor already exists.
          // This should never happen. Let's just quit.
          logError("Got two different block manager registrations on " + id.executorId)
          System.exit(1)
        case None =>
          blockManagerIdByExecutor(id.executorId) = id
      }
      blockManagerInfo(id) = new BlockManagerMasterActor.BlockManagerInfo(
        id, System.currentTimeMillis(), maxMemSize, slaveActor)
    }
  }

  private def updateBlockInfo(
      blockManagerId: BlockManagerId,
      blockId: BlockId,
      storageLevel: StorageLevel,
      memSize: Long,
      diskSize: Long) {

    if (!blockManagerInfo.contains(blockManagerId)) {
      if (blockManagerId.executorId == "<driver>" && !isLocal) {
        // We intentionally do not register the master (except in local mode),
        // so we should not indicate failure.
        sender ! true
      } else {
        sender ! false
      }
      return
    }

    if (blockId == null) {
      blockManagerInfo(blockManagerId).updateLastSeenMs()
      sender ! true
      return
    }

    blockManagerInfo(blockManagerId).updateBlockInfo(blockId, storageLevel, memSize, diskSize)

    var locations: mutable.HashSet[BlockManagerId] = null
    if (blockLocations.containsKey(blockId)) {
      locations = blockLocations.get(blockId)
    } else {
      locations = new mutable.HashSet[BlockManagerId]
      blockLocations.put(blockId, locations)
    }

    if (storageLevel.isValid) {
      locations.add(blockManagerId)
    } else {
      locations.remove(blockManagerId)
    }

    // Remove the block from master tracking if it has been removed on all slaves.
    if (locations.size == 0) {
      blockLocations.remove(blockId)
    }
    sender ! true
  }

  private def getLocations(blockId: BlockId): Seq[BlockManagerId] = {
    if (blockLocations.containsKey(blockId)) blockLocations.get(blockId).toSeq else Seq.empty
  }

  private def getLocationsMultipleBlockIds(blockIds: Array[BlockId]): Seq[Seq[BlockManagerId]] = {
    blockIds.map(blockId => getLocations(blockId))
  }

  private def getPeers(blockManagerId: BlockManagerId, size: Int): Seq[BlockManagerId] = {
    val peers: Array[BlockManagerId] = blockManagerInfo.keySet.toArray

    val selfIndex = peers.indexOf(blockManagerId)
    if (selfIndex == -1) {
      throw new SparkException("Self index for " + blockManagerId + " not found")
    }

    // Note that this logic will select the same node multiple times if there aren't enough peers
    Array.tabulate[BlockManagerId](size) { i => peers((selfIndex + i + 1) % peers.length) }.toSeq
  }
}


private[spark]
object BlockManagerMasterActor {

  case class BlockStatus(storageLevel: StorageLevel, memSize: Long, diskSize: Long)

  class BlockManagerInfo(
      val blockManagerId: BlockManagerId,
      timeMs: Long,
      val maxMem: Long,
      val slaveActor: ActorRef)
    extends Logging {

    private var _lastSeenMs: Long = timeMs
    private var _remainingMem: Long = maxMem

    // Mapping from block id to its status.
    private val _blocks = new JHashMap[BlockId, BlockStatus]

    logInfo("Registering block manager %s with %s RAM".format(
      blockManagerId.hostPort, Utils.bytesToString(maxMem)))

    def updateLastSeenMs() {
      _lastSeenMs = System.currentTimeMillis()
    }

    def updateBlockInfo(blockId: BlockId, storageLevel: StorageLevel, memSize: Long,
                        diskSize: Long) {

      updateLastSeenMs()

      if (_blocks.containsKey(blockId)) {
        // The block exists on the slave already.
        val originalLevel: StorageLevel = _blocks.get(blockId).storageLevel

        if (originalLevel.useMemory) {
          _remainingMem += memSize
        }
      }

      if (storageLevel.isValid) {
        // isValid means it is either stored in-memory or on-disk.
        _blocks.put(blockId, BlockStatus(storageLevel, memSize, diskSize))
        if (storageLevel.useMemory) {
          _remainingMem -= memSize
          logInfo("Added %s in memory on %s (size: %s, free: %s)".format(
            blockId, blockManagerId.hostPort, Utils.bytesToString(memSize),
            Utils.bytesToString(_remainingMem)))
        }
        if (storageLevel.useDisk) {
          logInfo("Added %s on disk on %s (size: %s)".format(
            blockId, blockManagerId.hostPort, Utils.bytesToString(diskSize)))
        }
      } else if (_blocks.containsKey(blockId)) {
        // If isValid is not true, drop the block.
        val blockStatus: BlockStatus = _blocks.get(blockId)
        _blocks.remove(blockId)
        if (blockStatus.storageLevel.useMemory) {
          _remainingMem += blockStatus.memSize
          logInfo("Removed %s on %s in memory (size: %s, free: %s)".format(
            blockId, blockManagerId.hostPort, Utils.bytesToString(blockStatus.memSize),
            Utils.bytesToString(_remainingMem)))
        }
        if (blockStatus.storageLevel.useDisk) {
          logInfo("Removed %s on %s on disk (size: %s)".format(
            blockId, blockManagerId.hostPort, Utils.bytesToString(blockStatus.diskSize)))
        }
      }
    }

    def removeBlock(blockId: BlockId) {
      if (_blocks.containsKey(blockId)) {
        _remainingMem += _blocks.get(blockId).memSize
        _blocks.remove(blockId)
      }
    }

    def remainingMem: Long = _remainingMem

    def lastSeenMs: Long = _lastSeenMs

    def blocks: JHashMap[BlockId, BlockStatus] = _blocks

    override def toString: String = "BlockManagerInfo " + timeMs + " " + _remainingMem

    def clear() {
      _blocks.clear()
    }
  }
}
