package util

import java.io.File
import util.MetricsBuilder._
import scala.io.Source
import spray.json._
import scala.collection.breakOut
import util.EventLogParser._
import java.io.PrintWriter
import java.io.FileWriter
import scala.math.BigDecimal

object StageDurationComparator {
  def main(args: Array[String]) {

    require(args.length == 2)

    val eventLogFile = new File(args(0))

    val resultFile = new File(args(1))

    val lines = Source.fromFile(eventLogFile).getLines().toList

    val parsed = lines
      .map(_.parseJson)
      .map(jsflatter(_))
      .filter(getString(_, ".Event", "") == "SparkListenerTaskEnd")

    val globalStartTime = parsed.map(getBigDecimal(_, ".Task Info.Launch Time")).min  
    
    val orderedStageIDs = parsed.map(getBigDecimal(_, ".Stage ID")).toSet.toList.sorted
    
    def getNewStageId(id : BigDecimal) : BigDecimal = {
      BigDecimal(orderedStageIDs.indexOf(id))
    }
    
    val grouped = parsed.groupBy(getString(_, ".Task Info.Host"))

    
    
    val ips = grouped.map(_._1).toList

    val s = for (ip <- ips) yield {

      val machineTasks = grouped.get(ip).get

      val groupedByStages = machineTasks.groupBy(getBigDecimal(_, ".Stage ID"))

      val data = groupedByStages.flatMap(group => {
        val stageId = group._1
        val tasks = group._2

        val end = tasks.map(getBigDecimal(_, ".Task Info.Finish Time")).max - globalStartTime
        val start = tasks.map(getBigDecimal(_, ".Task Info.Launch Time")).min - globalStartTime

        List(List(getNewStageId(stageId), start, end))

      }).toList
      data
    }
    val zipMapped = s.reduce((a, b) => {
      (a, b).zipped map(_ ++ _)
    })

    val content = zipMapped.map(_.mkString(", ")).mkString("\n")
    
    val out = new PrintWriter(new FileWriter(resultFile));
    out.print(content)
    out.close
    
  }
}