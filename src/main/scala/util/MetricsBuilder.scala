package util

import java.io.File
import java.io.PrintWriter
import java.io.FileWriter
import java.util.regex.Pattern
import scala.io.Source
import spray.json._
import util.EventLogParser._

object MetricsBuilder {
  def main(args: Array[String]) {

    require(args.length == 3)

    val root = new File(args(0))

    val prefix = args(1)

    val resultPath = args(2)

    var result = List[String]()

    for (subfolder <- root.listFiles() if subfolder.isDirectory() && subfolder.getName().startsWith(prefix)) {

      var line = List[String]()

      val partitionNumber = getPartitionNumberFromFileName(subfolder.getName())
      println(partitionNumber)

      line :+= partitionNumber.toString

      var subpath = subfolder.getAbsolutePath()
      if (subpath.charAt(subpath.length() - 1) != '/') {
        subpath += "/"
      }
      println(subpath)

      val eventLogFile = new File(subpath + "EVENT_LOG_1")

      val lines = Source.fromFile(eventLogFile).getLines().toList

      val parsed = lines.map(_.parseJson)
        .map(jsflatter(_))
        .filter(map => {
          map.get(".Event").isDefined && map.get(".Event").get.asInstanceOf[JsString].value == "SparkListenerTaskEnd"
        })

      def addMetric(metricName: String): Unit = {
        line ++= stats(getValuesForMetric(parsed, metricName)).map(_.toInt.toString)
      }

      count(".Stage ID")
      count(".Task Info.Task ID")

      addMetric(".Task Info.Getting Result Time")
      addMetric(".Task Metrics.Executor Run Time")
      addMetric(".Task Metrics.Executor Deserialize Time")
      addMetric(".Task Metrics.Result Size")
      addMetric(".Task Metrics.JVM GC Time")
      addMetric(".Task Metrics.Result Serialization Time")
      addMetric(".Task Metrics.Memory Bytes Spilled")
      addMetric(".Task Metrics.Disk Bytes Spilled")
      addMetric(".Task Metrics.Input Metrics.Bytes Read")
      addMetric(".Task Metrics.Shuffle Write Metrics.Shuffle Records Written")
      addMetric(".Task Metrics.Shuffle Write Metrics.Shuffle Write Time")
      addMetric(".Task Metrics.Shuffle Write Metrics.Shuffle Bytes Written")
      addMetric(".Task Metrics.Shuffle Read Metrics.Remote Bytes Read")

      def count(metricName: String): Unit = {
        line :+= countDifferent(parsed, metricName).toString

      }

      result :+= line.mkString(", ")
    }

    val out = new PrintWriter(new FileWriter(resultPath));
    out.print(result.mkString("\n"))
    out.close

  }

  def getPartitionNumberFromFileName(name: String): Int = {
    val m = Pattern.compile("-?\\d+-").matcher(name)
    if (m.find()) {
      val g = m.group()
      g.substring(1, g.length() - 1).toInt
    } else {
      throw new Exception(name)
    }

  }

  def countDifferent(data: List[Map[String, JsValue]], metricName: String): Int = {
    data.map(getBigDecimal(_, metricName)).toSet.toList.length
  }

  def getValuesForMetric(data: List[Map[String, JsValue]], metricName: String): List[BigDecimal] = {
    data.filter(_.contains(metricName)).map(getBigDecimal(_, metricName)).toList
  }

  def stats(data: List[BigDecimal]): List[BigDecimal] = {
    if (!data.isEmpty) {
      val average = BigDecimal(data.sum.intValue / data.length)
      val max = data.max
      val min = data.min
      var percentale95 = 1.96 * stddev(data) / Math.sqrt(data.length)
      val minus95 = average - percentale95
      val plus95 = average + percentale95
      List(min, minus95, average, plus95, max)
    } else List(0, 0, 0, 0, 0)
  }

  def stddev(arr: List[BigDecimal]): BigDecimal = {
    var aver = arr.sum / arr.length
    Math.sqrt(
      arr.toList.map(x => Math.pow(x.toDouble - aver.toDouble, 2)).sum / arr.length)
  }

}