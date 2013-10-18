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

import java.util.Date

import javax.servlet.http.HttpServletRequest

import scala.xml.Node

import org.apache.spark.{ExceptionFailure}
import org.apache.spark.executor.TaskMetrics
import org.apache.spark.ui.UIUtils._
import org.apache.spark.ui.Page._
import org.apache.spark.util.{Utils, Distribution}
import org.apache.spark.scheduler.TaskInfo

/** Page showing statistics and task list for a given stage */
private[spark] class StagePage(parent: JobProgressUI) {
  def listener = parent.listener
  val dateFmt = parent.dateFmt

  def render(request: HttpServletRequest): Seq[Node] = {
    listener.synchronized {
      val stageId = request.getParameter("id").toInt
      val now = System.currentTimeMillis()

      if (!listener.stageToTaskInfos.contains(stageId)) {
        val content =
          <div>
            <h4>Summary Metrics</h4> No tasks have started yet
            <h4>Tasks</h4> No tasks have started yet
          </div>
        return headerSparkPage(content, parent.sc, "Details for Stage %s".format(stageId), Stages)
      }

      val tasks = listener.stageToTaskInfos(stageId).toSeq.sortBy(_._1.launchTime)

      val numCompleted = tasks.count(_._1.finished)
      val shuffleReadBytes = listener.stageToShuffleRead.getOrElse(stageId, 0L)
      val hasShuffleRead = shuffleReadBytes > 0
      val shuffleWriteBytes = listener.stageToShuffleWrite.getOrElse(stageId, 0L)
      val hasShuffleWrite = shuffleWriteBytes > 0

      var activeTime = 0L
      listener.stageToTasksActive(stageId).foreach(activeTime += _.timeRunning(now))

      val summary =
        <div>
          <ul class="unstyled">
            <li>
              <strong>CPU time: </strong>
              {parent.formatDuration(listener.stageToTime.getOrElse(stageId, 0L) + activeTime)}
            </li>
            {if (hasShuffleRead)
              <li>
                <strong>Shuffle read: </strong>
                {Utils.bytesToString(shuffleReadBytes)}
              </li>
            }
            {if (hasShuffleWrite)
              <li>
                <strong>Shuffle write: </strong>
                {Utils.bytesToString(shuffleWriteBytes)}
              </li>
            }
          </ul>
        </div>

      val taskHeaders: Seq[String] =
        Seq("Task ID", "Status", "Locality Level", "Executor", "Launch Time", "Duration") ++
        Seq("GC Time") ++
        {if (hasShuffleRead) Seq("Shuffle Read")  else Nil} ++
        {if (hasShuffleWrite) Seq("Shuffle Write") else Nil} ++
        Seq("Errors")

      val taskTable = listingTable(taskHeaders, taskRow(hasShuffleRead, hasShuffleWrite), tasks)

      // Excludes tasks which failed and have incomplete metrics
      val validTasks = tasks.filter(t => t._1.status == "SUCCESS" && (t._2.isDefined))

      val summaryTable: Option[Seq[Node]] =
        if (validTasks.size == 0) {
          None
        }
        else {
          val serviceTimes = validTasks.map{case (info, metrics, exception) =>
            metrics.get.executorRunTime.toDouble}
          val serviceQuantiles = "Duration" +: Distribution(serviceTimes).get.getQuantiles().map(
            ms => parent.formatDuration(ms.toLong))

          def getQuantileCols(data: Seq[Double]) =
            Distribution(data).get.getQuantiles().map(d => Utils.bytesToString(d.toLong))

          val shuffleReadSizes = validTasks.map {
            case(info, metrics, exception) =>
              metrics.get.shuffleReadMetrics.map(_.remoteBytesRead).getOrElse(0L).toDouble
          }
          val shuffleReadQuantiles = "Shuffle Read (Remote)" +: getQuantileCols(shuffleReadSizes)

          val shuffleWriteSizes = validTasks.map {
            case(info, metrics, exception) =>
              metrics.get.shuffleWriteMetrics.map(_.shuffleBytesWritten).getOrElse(0L).toDouble
          }
          val shuffleWriteQuantiles = "Shuffle Write" +: getQuantileCols(shuffleWriteSizes)

          val listings: Seq[Seq[String]] = Seq(serviceQuantiles,
            if (hasShuffleRead) shuffleReadQuantiles else Nil,
            if (hasShuffleWrite) shuffleWriteQuantiles else Nil)

          val quantileHeaders = Seq("Metric", "Min", "25th percentile",
            "Median", "75th percentile", "Max")
          def quantileRow(data: Seq[String]): Seq[Node] = <tr> {data.map(d => <td>{d}</td>)} </tr>
          Some(listingTable(quantileHeaders, quantileRow, listings, fixedWidth = true))
        }

      val content =
        summary ++
        <h4>Summary Metrics for {numCompleted} Completed Tasks</h4> ++
        <div>{summaryTable.getOrElse("No tasks have reported metrics yet.")}</div> ++
        <h4>Tasks</h4> ++ taskTable;

      headerSparkPage(content, parent.sc, "Details for Stage %d".format(stageId), Stages)
    }
  }


  def taskRow(shuffleRead: Boolean, shuffleWrite: Boolean)
             (taskData: (TaskInfo, Option[TaskMetrics], Option[ExceptionFailure])): Seq[Node] = {
    def fmtStackTrace(trace: Seq[StackTraceElement]): Seq[Node] =
      trace.map(e => <span style="display:block;">{e.toString}</span>)
    val (info, metrics, exception) = taskData

    val duration = if (info.status == "RUNNING") info.timeRunning(System.currentTimeMillis())
      else metrics.map(m => m.executorRunTime).getOrElse(1)
    val formatDuration = if (info.status == "RUNNING") parent.formatDuration(duration)
      else metrics.map(m => parent.formatDuration(m.executorRunTime)).getOrElse("")
    val gcTime = metrics.map(m => m.jvmGCTime).getOrElse(0L)

    <tr>
      <td>{info.taskId}</td>
      <td>{info.status}</td>
      <td>{info.taskLocality}</td>
      <td>{info.host}</td>
      <td>{dateFmt.format(new Date(info.launchTime))}</td>
      <td sorttable_customkey={duration.toString}>
        {formatDuration}
      </td>
      <td sorttable_customkey={gcTime.toString}>
        {if (gcTime > 0) parent.formatDuration(gcTime) else ""}
      </td>
      {if (shuffleRead) {
        <td>{metrics.flatMap{m => m.shuffleReadMetrics}.map{s =>
          Utils.bytesToString(s.remoteBytesRead)}.getOrElse("")}</td>
      }}
      {if (shuffleWrite) {
        <td>{metrics.flatMap{m => m.shuffleWriteMetrics}.map{s =>
          Utils.bytesToString(s.shuffleBytesWritten)}.getOrElse("")}</td>
      }}
      <td>{exception.map(e =>
        <span>
          {e.className} ({e.description})<br/>
          {fmtStackTrace(e.stackTrace)}
        </span>).getOrElse("")}
      </td>
    </tr>
  }
}
