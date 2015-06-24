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

object TaskDurationComparator {
  def main(args: Array[String]) {

    val eventLogFile = new File(args(0))

    val resultFileTask = new File(args(1))
    
    val resultFileStages = new File(args(2))

    val minStageDuration = if(args.length == 4) args(3).toInt else 0

    val lines = Source.fromFile(eventLogFile).getLines().toList

     val globalStartTime = lines
      .map(_.parseJson)
      .map(jsflatter(_))
      .filter(getString(_, ".Event") == "SparkListenerStageCompleted")
      .map(getBigDecimal(_, ".Stage Info.Submission Time")).min

    
    val stages = lines
      .map(_.parseJson)
      .map(jsflatter(_))
      .filter(getString(_, ".Event") == "SparkListenerStageCompleted")

    val longStages = stages.map(x => {
      val stageId = getBigDecimal(x, ".Stage Info.Stage ID")

      val submissionTime = getBigDecimal(x, ".Stage Info.Submission Time") - globalStartTime
      val completionTime = getBigDecimal(x, ".Stage Info.Completion Time") - globalStartTime
      (stageId, completionTime - submissionTime, submissionTime, completionTime )
    }).toList.filter(_._2 > minStageDuration).sortWith((a,b) => a._1 < b._1)

    println("There are " + longStages.length + " long stages; longer than " + minStageDuration + " ms.")
    
    if(longStages.isEmpty){
      println("There are not so long stages.")
      System.exit(0)
    }
    
    var out = new PrintWriter(new FileWriter(resultFileStages));
    
    var content = longStages.map(s => List(s._1, s._3, s._4).mkString(", ")).toList.mkString("\n")
    out.print(content)
    out.flush()
    
    val tasks = lines
      .map(_.parseJson)
      .map(jsflatter(_))
      .filter(getString(_, ".Event") == "SparkListenerTaskEnd")
      .filter(task => {
        longStages.exists(_._1 == getBigDecimal(task, ".Stage ID"))
      })


    val data = tasks.map(task => {

      val taskId = getBigDecimal(task, ".Task Info.Task ID")
      val end = getBigDecimal(task, ".Task Info.Finish Time") - globalStartTime
      val start = getBigDecimal(task, ".Task Info.Launch Time") - globalStartTime

      List(taskId, start, end)

    }).toList.sortWith((a,b) => a(0) < b(0))

    content = data.map(_.mkString(", ")).mkString("\n")
    
    out = new PrintWriter(new FileWriter(resultFileTask));
    out.print(content)
    
    
    out.close

  }

}