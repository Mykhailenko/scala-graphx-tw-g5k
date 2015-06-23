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
import akka.dispatch.Foreach

object StageDurationTrueComparator {
  def main(args: Array[String]) {

    require(args.length == 2)

    val eventLogFile = new File(args(0))

    val resultFile = new File(args(1))

    val lines = Source.fromFile(eventLogFile).getLines().toList

    val parsed = lines
      .map(_.parseJson)
      .map(jsflatter(_))
      .filter(getString(_, ".Event") == "SparkListenerStageCompleted")

    val globalStartTime = parsed.map(getBigDecimal(_, ".Stage Info.Submission Time")).min

    val orderedStageIDs = parsed.map(getBigDecimal(_, ".Stage Info.Stage ID")).toSet.toList.sorted

    def getNewStageId(id: BigDecimal): BigDecimal = {
      BigDecimal(orderedStageIDs.indexOf(id))
    }

    val stages = parsed.map(x => {
      val stageId = getBigDecimal(x, ".Stage Info.Stage ID")

      val submissionTime = getBigDecimal(x, ".Stage Info.Submission Time") - globalStartTime
      val completionTime = getBigDecimal(x, ".Stage Info.Completion Time") - globalStartTime
      List(getNewStageId(stageId), submissionTime, completionTime)
    }).toList

    val stagesThatOverlap = stages.filter(stage => {
      overlapp(stage, stages)
    })
       
    val content = stages.map(_.mkString(", ")).mkString("\n")

    var out = new PrintWriter(new FileWriter(resultFile));
    out.print(content)
    out.flush
    
     out = new PrintWriter(new FileWriter(resultFile + "overlap"));
    out.print(stagesThatOverlap.map(_.mkString(", ")).mkString("\n"))
    out.close

  }
  def overlapp(stage: List[BigDecimal], list: List[List[BigDecimal]]): Boolean = {
    list.exists(elem => {
      if (elem(0) == stage(0))
        false
      else {
        ((elem(1) <= stage(1) && stage(1) <= elem(2))
          ||
          (elem(1) <= stage(2) && stage(2) <= elem(2)))
      }
    })
  }

}