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

package org.apache.spark.network

import java.nio.ByteBuffer
import java.net.InetAddress
import org.apache.spark.SparkConf

private[spark] object ReceiverTest {
  def main(args: Array[String]) {
    val manager = new ConnectionManager(9999, new SparkConf)
    println("Started connection manager with id = " + manager.id)

    manager.onReceiveMessage((msg: Message, id: ConnectionManagerId) => {
      /*println("Received [" + msg + "] from [" + id + "] at " + System.currentTimeMillis)*/
      val buffer = ByteBuffer.wrap("response".getBytes)
      Some(Message.createBufferMessage(buffer, msg.id))
    })
    Thread.currentThread.join()
  }
}

