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

object Overlapp {
  def main(args: Array[String]) {

    require(args.length == 1)

    val eventLogFile = new File(args(0))

    val lines = Source.fromFile(eventLogFile).getLines().toList

    val parsed = lines
      .map(_.parseJson)
      .map(jsflatter(_))
      .filter(getString(_, ".Event", "") == "SparkListenerTaskEnd")

    val groupedByStages = parsed.groupBy(getBigDecimal(_, ".Stage ID"))

    val data = groupedByStages.map(group => {
      val stageId = group._1
      val tasks = group._2

      val end = tasks.map(getBigDecimal(_, ".Task Info.Finish Time")).max
      val start = tasks.map(getBigDecimal(_, ".Task Info.Launch Time")).min

      (start, end)

    }).toList

    val over = data.filter(x => overlapp(x, data))

    println("Overlapp = " + !over.isEmpty)
    println("Size = " + over.length)
  }

  def overlapp(pair: (BigDecimal, BigDecimal), list: List[(BigDecimal, BigDecimal)]): Boolean = {
    !list.filter(elem => {
      if (elem._1 == pair._1 && elem._2 == pair._2)
        false
      else {
        ((elem._1 <= pair._1 && pair._1 <= elem._2)
            ||
            (elem._1 <= pair._2 && pair._2 <= elem._2))
      }
    }).isEmpty
  }

}