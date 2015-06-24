package util

import java.io.File
import java.io.PrintWriter
import java.io.FileWriter
import java.util.regex.Pattern
import scala.io.Source
import spray.json._
import util.EventLogParser._

object MetricsBuilder {

  val partitioners = Array("randomvertexcut") //, "canonicalrandomvertexcut", "edgepartition1d", "edgepartition2d", "hybridcut", "hybridcutplus")

  var headers = List[String]()

  def addHeader(header: String): Unit = {
    if (!headers.contains(header)) {
      headers :+= header
    }
  }

  def main(args: Array[String]) {

    require(args.length == 4)

    val root = new File(args(0))

    val resultPath = if (args(1).charAt(args(1).length - 1) != '/') args(1) + '/' else args(1)

    val coresFrom = args(2).toInt

    val coresTo = args(3).toInt

    for (partitioner <- partitioners) {

      var result = List[String]()
      for (core <- coresFrom to coresTo) {
        val createSubstring = s"$partitioner-$core"
        val partitionNumber = core
        var totalFlatted: List[Map[String, JsValue]] = List()

        for (subfolder <- root.listFiles() if subfolder.isDirectory() && subfolder.getName().contains(createSubstring)) {
          var subpath = subfolder.getAbsolutePath()
          if (subpath.charAt(subpath.length() - 1) != '/') {
            subpath += "/"
          }
          val eventLogFile = new File(subpath + "EVENT_LOG_1")

          val lines = Source.fromFile(eventLogFile).getLines().toList

          val flatted = lines.map(_.parseJson)
            .map(jsflatter(_))

          totalFlatted ++= flatted
        }

        println("totalFlatted.length = " + totalFlatted.length)
        val parsed = totalFlatted.filter(getTasks(_))

        var line = List[String]()
        addHeader("Partition number")
        line :+= partitionNumber.toString

        def addMetric(metricName: String): Unit = {
          val h = List[String]("min", "min99%", "average", "max99%", "max")
          h.map(x => s"$metricName.$x").foreach(xx => addHeader(xx))
          line ++= stats(getValuesForMetric(parsed, metricName)).map(_.toInt.toString)
        }

        def count(metricName: String): Unit = {
          addHeader(metricName)
          line :+= countDifferent(parsed, metricName).toString
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

        val groupedByStages = parsed.groupBy(getBigDecimal(_, ".Stage ID")).toList
        val waiting = groupedByStages.map(group => {
          val stageId = group._1
          val tasks = group._2

          val slowest = tasks.maxBy(getBigDecimal(_, ".Task Info.Finish Time"))
          val fastest = tasks.minBy(getBigDecimal(_, ".Task Info.Launch Time"))

          val endOfSlowest = getBigDecimal(slowest, ".Task Info.Finish Time")
          val endOfFastest = getBigDecimal(fastest, ".Task Info.Finish Time")

          val latency = endOfSlowest - endOfFastest
          latency

        }).toList

        addHeader("Latency.min")
        addHeader("Latency.min99%")
        addHeader("Latency.average")
        addHeader("Latency.max99%")
        addHeader("Latency.max")
        line ++= stats(waiting).map(_.toInt.toString)
        val stagesCompleted = totalFlatted.filter(getString(_, ".Event") == "SparkListenerStageCompleted")

        val stagesDurationTrue = stagesCompleted.map(x => {
          val submissionTime = getBigDecimal(x, ".Stage Info.Submission Time")
          val completionTime = getBigDecimal(x, ".Stage Info.Completion Time")
          val stageDurationTrue = completionTime - submissionTime
          stageDurationTrue
        })

        addHeader("Stage duration.min")
        addHeader("Stage duration.min99%")
        addHeader("Stage duration.average")
        addHeader("Stage duration.max99%")
        addHeader("Stage duration.max")
        line ++= stats(stagesDurationTrue).map(_.toInt.toString)
        result :+= line.mkString(", ")
      }
      val headersZipped = headers.zipWithIndex.map(x => x._2 + " : " + x._1)
      val content = "#" + headersZipped.mkString(", ") + "\n" + result.mkString("\n")
      val out = new PrintWriter(new FileWriter(resultPath + partitioner + ".csv"));
      out.print(content)
      out.close
    }

  }

  def getTasks(map: Map[String, spray.json.JsValue]): Boolean = {
    getString(map, ".Event") == "SparkListenerTaskEnd"
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
      // confidence level 90% 	95% 	99%
      //             zα/2 1.645 1.96 	2.575
      var percentale95 = 2.575 * stddev(data) / Math.sqrt(data.length)
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