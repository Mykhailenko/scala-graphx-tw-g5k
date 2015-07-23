package util

import util.MetricsBuilder._
import java.io.File
import java.io.PrintWriter
import java.io.FileWriter
import util.EventLogParser._

object Variability {

  val metrics = List(".applicationTime.time", ".AlgorithExecution.time", ".GraphLoading.time", ".Partitioning.time", ".ResultSaving.time")

  def main(args: Array[String]) {
    require(args.length == 4)
    val root = new File(args(0))
    val resultFile = new File(args(1))
    val FROM = args(2).toInt
    val TO = args(3).toInt
    var content = ""
    for (N <- FROM until TO) {
      val list = jsonWorker(root, s".*", ".*", ".*", N.toString)
      content += s"$N\n"
      for (metric <- metrics) {
        content += metric + ", " + statsInt(list.map(getString(_, metric).toInt)).mkString(", ") + "\n"
      }

    }
    val out = new PrintWriter(new FileWriter(resultFile))
    out.print(content)
    out.close

  }
}