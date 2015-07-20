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

object Delay {
  def main(args: Array[String]) {
	  val file = new File(args(0))
	  
	  val values = for(f <- listFiles(file).filter(_.getName() == "EVENT_LOG_1")) yield {
	    getDelay(f)
	  }
	  println(stats(values.toList).mkString(", "))
  }
  def getDelay(file : File) = {
    println(file.getAbsolutePath())
    val lines = Source.fromFile(file).getLines().toList

    val flat = lines
      .map(_.parseJson)
      .map(jsflatter(_))

    val allTasks = flat.filter(getString(_, ".Event", "") == "SparkListenerTaskEnd")
    println("allTasks: " + allTasks.length)
    val firstTast = allTasks.map(getBigDecimal(_, ".Task Info.Launch Time")).min
    println("firstTast: " + firstTast)

    val allStages = flat.filter(getString(_, ".Event", "") == "SparkListenerStageCompleted")
    println("allStages: " + allStages.length)
    val firstStage = allStages.map(getBigDecimal(_, ".Stage Info.Submission Time")).min
    println("firstStage: " + firstStage)
    
    
    val diff = firstTast - firstStage
    diff
  }
}