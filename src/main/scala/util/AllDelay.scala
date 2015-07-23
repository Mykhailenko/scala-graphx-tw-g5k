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

object AllDelay {
  def main(args: Array[String]) {
    val file = new File(args(0))

    val data = getDelay(file)
    
     var out = new PrintWriter(new FileWriter(args(1)));
    
    var content = data.map(x => x._1 + ", " + x._2).mkString("\n")
    out.print(content)
    out.flush()
  }
  def getDelay(file: File) = {
    println(file.getAbsolutePath())
    val lines = Source.fromFile(file).getLines().toList

    val flat = lines
      .map(_.parseJson)
      .map(jsflatter(_))

    val allStages = flat.filter(getString(_, ".Event", "") == "SparkListenerStageCompleted")
    					.map(x => (getBigDecimal(x, ".Stage Info.Stage ID" ), getBigDecimal(x, ".Stage Info.Submission Time").toInt))
    					.toMap
    					    

    
//    groupBy(getBigDecimal(_, ".Stage ID")).toList
    val allTasks = flat.filter(getString(_, ".Event", "") == "SparkListenerTaskEnd")
    					.groupBy(getBigDecimal(_, ".Stage ID")).toList
    					.map(x => {
    					  val listofmaps = x._2
    					  val mintime = listofmaps.map(getBigDecimal(_, ".Task Info.Launch Time").toInt).min
    					  (x._1, mintime)
    					})

    println(allStages.size)
    println(allTasks.size)
    					
    val diffs = allTasks.map(x => {
      val diff = x._2 - allStages(x._1);
      (x._1.toInt, diff) 
    })

    diffs
  }
}