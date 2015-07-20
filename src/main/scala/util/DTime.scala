package util

import java.io.File
import util.MetricsBuilder._
import java.io.PrintWriter
import java.io.FileWriter
import util.EventLogParser._

object DTime {
  val metric = ".Task Metrics.Executor Deserialize Time"
  def main(args: Array[String]) {
    val root = new File(args(0))

    val out = new PrintWriter(new FileWriter(args(1)));

    val files = listOfFilesEventLog(root)
    for (eventLogFile <- files) {
      val meta = getMetainfoFromEventLogFile(eventLogFile)
      val flatted = parseEventLogFile(eventLogFile)
      val filtered = flatted.filter(x => x.get(metric).isDefined)
      val mapped = filtered.map(getBigDecimal(_, metric).toInt)
      val long = mapped.filter(_ > 9000).size
      val all = mapped.size
      val max = mapped.max

      val graph = getString(meta, ".graph")
      val version = getString(meta, ".version")
      val part = getString(meta, ".partitioner")
      val cores = getBigDecimal(meta, ".cores")

      out.println(List(graph, version, part, cores, max, long, all).mkString(", "))

    }

    out.close
  }
}