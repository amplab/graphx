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

package org.apache.spark.scheduler.cluster

import java.nio.ByteBuffer

import org.apache.spark.TaskState.TaskState
import org.apache.spark.scheduler.TaskDescription
import org.apache.spark.util.{Utils, SerializableBuffer}


private[spark] sealed trait CoarseGrainedClusterMessage extends Serializable

private[spark] object CoarseGrainedClusterMessages {

  // Driver to executors
  case class LaunchTask(task: TaskDescription) extends CoarseGrainedClusterMessage

  case class KillTask(taskId: Long, executor: String) extends CoarseGrainedClusterMessage

  case class RegisteredExecutor(sparkProperties: Seq[(String, String)])
    extends CoarseGrainedClusterMessage

  case class RegisterExecutorFailed(message: String) extends CoarseGrainedClusterMessage

  // Executors to driver
  case class RegisterExecutor(executorId: String, hostPort: String, cores: Int)
    extends CoarseGrainedClusterMessage {
    Utils.checkHostPort(hostPort, "Expected host port")
  }

  case class StatusUpdate(executorId: String, taskId: Long, state: TaskState,
    data: SerializableBuffer) extends CoarseGrainedClusterMessage

  object StatusUpdate {
    /** Alternate factory method that takes a ByteBuffer directly for the data field */
    def apply(executorId: String, taskId: Long, state: TaskState, data: ByteBuffer)
      : StatusUpdate = {
      StatusUpdate(executorId, taskId, state, new SerializableBuffer(data))
    }
  }

  // Internal messages in driver
  case object ReviveOffers extends CoarseGrainedClusterMessage

  case object StopDriver extends CoarseGrainedClusterMessage

  case class RemoveExecutor(executorId: String, reason: String) extends CoarseGrainedClusterMessage

}
