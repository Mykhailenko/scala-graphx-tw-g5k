package util

import util.MetricsBuilder._
import java.io.File
import java.io.PrintWriter
import java.io.FileWriter
import util.EventLogParser._

object CumulativeDistribution {
  def main(args: Array[String]) {
	
    val path = new File(args(0))
	
    val data = cumulativeParsing(path, ".*twitter.*", ".*", ".*", ".*", ".Task Metrics.Executor Deserialize Time")
    
    val content = data.map(x => x._1 + ", " + x._2).mkString("\n")
    
    val out = new PrintWriter(new FileWriter(path.getAbsolutePath() + "/edu"));
    out.print(content)
    out.close
    
  }
}