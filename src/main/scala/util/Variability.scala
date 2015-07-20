package util

import util.MetricsBuilder._
import java.io.File
import java.io.PrintWriter
import java.io.FileWriter
import util.EventLogParser._

object Variability {
  def main(args: Array[String]) {
    require(args.length == 3)
    val root = new File(args(0))
    val graphName = args(1)
    val resultFile = new File(args(2))
    
    val list = jsonWorker(root, s".*$graphName.*", ".*", ".*", ".*")
    val metrics = List(".applicationTime.time", ".AlgorithExecution.time", ".GraphLoading.time", ".Partitioning.time", ".ResultSaving.time")
    
    val content = metrics.map(metric => {
      metric + ": " + stats(list.map(m => BigDecimal(getString(m, metric)))).map(_.toInt).mkString(", ")
    }).mkString("\n")
    
    val out = new PrintWriter(new FileWriter(resultFile))
    out.print(content)
    out.close
      
  }
}