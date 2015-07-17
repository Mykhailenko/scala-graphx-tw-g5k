package util

import util.MetricsBuilder._
import java.io.File
import java.io.PrintWriter
import java.io.FileWriter
import util.EventLogParser._

object CumulativeDistribution {
  def main(args: Array[String]) {

    val threshhold = 1000

    val path = new File(args(0))

    val vals = values(path, ".*", ".*", ".*", ".*",  ".Task Metrics.Executor Deserialize Time")

    val distr = cumulativeDistribution(vals)
    val content = distr.map(x => x._1 + ", " + x._2).mkString("\n")

    var out = new PrintWriter(new FileWriter(path.getAbsolutePath() + "/all"));
    out.print(content)
    out.close

    val numberofAllTasks = vals.map(_._2).reduce(_ + _)

    println("numberofAllTasks: " + numberofAllTasks)

    val longs = vals.filter(_._1 > threshhold)

    if (!longs.isEmpty) {
      val distrLong = cumulativeDistribution(longs)

      val contentLong = distrLong.map(x => x._1 + ", " + x._2).mkString("\n")

      out = new PrintWriter(new FileWriter(path.getAbsolutePath() + "/long"));
      out.print(contentLong)
      out.close

      val numberOfLongTasks = longs.map(_._2).reduce(_ + _)

      println(s"; including longer than $threshhold second: " + numberOfLongTasks)

      out = new PrintWriter(new FileWriter(path.getAbsolutePath() + "/longvals"));
      out.print(longs.map(x => x._1).mkString("\n"))
      out.close
    }

  }
}