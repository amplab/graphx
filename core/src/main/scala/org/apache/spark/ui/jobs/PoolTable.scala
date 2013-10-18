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

package org.apache.spark.ui.jobs

import scala.collection.mutable.HashMap
import scala.collection.mutable.HashSet
import scala.xml.Node

import org.apache.spark.scheduler.{Schedulable, Stage}
import org.apache.spark.ui.UIUtils

/** Table showing list of pools */
private[spark] class PoolTable(pools: Seq[Schedulable], listener: JobProgressListener) {

  var poolToActiveStages: HashMap[String, HashSet[Stage]] = listener.poolToActiveStages

  def toNodeSeq(): Seq[Node] = {
    listener.synchronized {
      poolTable(poolRow, pools)
    }
  }

  private def poolTable(makeRow: (Schedulable, HashMap[String, HashSet[Stage]]) => Seq[Node],
    rows: Seq[Schedulable]
    ): Seq[Node] = {
    <table class="table table-bordered table-striped table-condensed sortable table-fixed">
      <thead>
        <th>Pool Name</th>
        <th>Minimum Share</th>
        <th>Pool Weight</th>
        <th>Active Stages</th>
        <th>Running Tasks</th>
        <th>SchedulingMode</th>
      </thead>
      <tbody>
        {rows.map(r => makeRow(r, poolToActiveStages))}
      </tbody>
    </table>
  }

  private def poolRow(p: Schedulable, poolToActiveStages: HashMap[String, HashSet[Stage]])
    : Seq[Node] = {
    val activeStages = poolToActiveStages.get(p.name) match {
      case Some(stages) => stages.size
      case None => 0
    }
    <tr>
      <td><a href={"%s/stages/pool?poolname=%s".format(UIUtils.prependBaseUri(),p.name)}>{p.name}</a></td>
      <td>{p.minShare}</td>
      <td>{p.weight}</td>
      <td>{activeStages}</td>
      <td>{p.runningTasks}</td>
      <td>{p.schedulingMode}</td>
    </tr>
  }
}

