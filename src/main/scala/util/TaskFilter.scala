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

object TaskFilter {
  def main(args: Array[String]) {

    require(args.length == 3)

    val eventLogFile = new File(args(0))

    val resultFile = new File(args(1))

    val minStageDuration = args(2).toInt

    val lines = Source.fromFile(eventLogFile).getLines().toList

    val content = lines
      .map(_.parseJson)
      .map(jsflatter(_))
      .filter(getString(_, ".Event") == "SparkListenerTaskEnd")
      .filter(task => {
        val end = getBigDecimal(task, ".Task Info.Finish Time")
        val start = getBigDecimal(task, ".Task Info.Launch Time")
        val duration = end - start
        duration > minStageDuration
      })
      .map(x => {
        x.toList.map(pair => pair._1 + " : " + pair._2).mkString("\n")
      }).mkString("\n\n")

    val out = new PrintWriter(new FileWriter(resultFile));
    out.print(content)

    out.close
  }
}