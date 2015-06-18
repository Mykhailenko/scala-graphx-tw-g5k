package util

import org.apache.spark.SparkConf
import sys.process._
import org.apache.spark.SparkContext
import scala.io.Source
import spray.json._
import DefaultJsonProtocol._
import java.io.PrintWriter
import java.io.FileWriter
import java.io.File

object EventLogParser {

  def main(args: Array[String]) {
    val numberOfParams = 2
    require(args.length == numberOfParams, s"Wrong argument number. Should be $numberOfParams. Usage: <path_to_eventlog_dir> <go_inside_each_directory>")

    val mainPath = args(0)
    val mode = args(1).toBoolean

    val sroot = new File(mainPath)
    require(sroot.isDirectory(), "It is not a directory " + mainPath)

    if (mode) {
      val listFiles = sroot.listFiles()

      for (root <- listFiles if root.isDirectory()) {
        print("parse" + root.getAbsolutePath());
        workWithDirectory(root.getAbsolutePath())
        println("...parsed")
      }

    } else {
      workWithDirectory(mainPath)
    }

  }
  def workWithDirectory(path: String) {
    var rootPath = path
    if (rootPath.charAt(rootPath.length() - 1) != '/') {
      rootPath += "/"
    }

    val eventLogFile = new File(rootPath + "EVENT_LOG_1")

    val lines = Source.fromFile(eventLogFile).getLines().toList

    val rawdata = lines.map(_.parseJson)
      .map(jsflatter(_))
    
    val parsed = rawdata
      .filter(getString(_, ".Event", "") == "SparkListenerTaskEnd")

    createFile(eventLogFile.getAbsolutePath() + "ALL.csv", createContentOfCSVFile(rawdata))
    createFile(eventLogFile.getAbsolutePath() + ".csv", createContentOfCSVFile(parsed))

    val startTime = parsed.map(getBigDecimal(_, ".Task Info.Launch Time")).min

    val linesForResultTask = parsed
      .filter(getString(_, ".Task Type", "") == "ResultTask")
      .map(mapMapsToUsefulArray(_))

    val linesForShuffleTask = parsed
      .filter(getString(_, ".Task Type", "") == "ShuffleMapTask")
      .map(mapMapsToUsefulArray(_)).sortWith((a, b) => a(1).toLong < b(1).toLong)

    val linesForStages = parsed.toList.groupBy(getBigDecimal(_, ".Stage ID")).toList.map(x => {
      val listOfMaps = x._2
      val end = listOfMaps.map(getBigDecimal(_, ".Task Info.Finish Time")).max
      val start = listOfMaps.map(getBigDecimal(_, ".Task Info.Launch Time")).min
      val duration = end - start

      val durationMax = listOfMaps.map(a => {
        getBigDecimal(a, ".Task Info.Finish Time") - getBigDecimal(a, ".Task Info.Launch Time")
      }).max

      val bytesReadAdnWrie = listOfMaps.map(a => {
        (getBigDecimal(a, ".Task Metrics.Shuffle Read Metrics.Remote Bytes Read", 0) +
          getBigDecimal(a, ".Task Metrics.Shuffle Write Metrics.Shuffle Bytes Written", 0)) / 10240
      }).sum

      val bytesReadAdnWrieMax = listOfMaps.map(a => {
        (getBigDecimal(a, ".Task Metrics.Shuffle Read Metrics.Remote Bytes Read", 0) +
          getBigDecimal(a, ".Task Metrics.Shuffle Write Metrics.Shuffle Bytes Written", 0)) / 10240
      }).max

      val shuffleBlockTime = listOfMaps.map(getBigDecimal(_, ".Task Metrics.Shuffle Write Metrics.Shuffle Write Time", 0)).sum / 10000

      val shuffleBlockTimeMax = listOfMaps.map(getBigDecimal(_, ".Task Metrics.Shuffle Write Metrics.Shuffle Write Time", 0)).max / 10000

      //      val duration = maxFinishTime - minLaunchTime
      val maxTaskId = x._2.map(a => a.get(".Task Info.Task ID").get.asInstanceOf[JsNumber].value).max
      Array(maxTaskId, duration, durationMax, bytesReadAdnWrie, bytesReadAdnWrieMax, shuffleBlockTime, shuffleBlockTimeMax)
    }).sortWith((a, b) => a(0) < b(0))

    createFile(rootPath + "resultTask", linesForResultTask.mkString("\n"))
    createFile(rootPath + "shuffleTask", linesForShuffleTask.map(_.mkString(", ")).mkString("\n"))
    createFile(rootPath + "stages", linesForStages.map(_.mkString(", ")).mkString("\n"))
    createFile(rootPath + "taskstages.gnu", String.format(gnuplotTemplate, rootPath))
    createFile(rootPath + "generalStats", createContent(parsed))
  }

  def createContent(parsed: List[Map[String, JsValue]]): String = {

    val numberOfStages = parsed.map(getBigDecimal(_, ".Stage ID")).toSet.toList.length
    var result: List[String] = List()
    result +:= s"numberOfStages: $numberOfStages"
    val numberOfTasks = parsed.length
    result +:= s"numberOfTasks: $numberOfTasks"

//          getBigDecimal(x, ".Task Info.Finish Time") - getBigDecimal(x, ".Task Info.Launch Time"),

    val durationOfAllTheTasks = parsed.map(getBigDecimal(_, ".Task Info.Finish Time")).max - parsed.map(getBigDecimal(_, ".Task Info.Launch Time")).min
    result +:= s"durationOfAllTheTasks: $durationOfAllTheTasks"
    
    val succedTasks = parsed.filter(getString(_, ".Task End Reason.Reason") == "Success")

    val numberSuccesShuffleTasks = succedTasks.filter(getString(_, ".Task Type") == "ShuffleMapTask").length
    result +:= s"numberSuccesShuffleTasks : $numberSuccesShuffleTasks "
    val numberSuccesResultTasks = succedTasks.filter(getString(_, ".Task Type") == "ResultTask").length
    result +:= s"numberSuccesResultTasks : $numberSuccesResultTasks "

    val timeSuccesSuccesShuffleTasksExecutorRunTime = succedTasks.filter(getString(_, ".Task Type") == "ShuffleMapTask")
      .map(getBigDecimal(_, ".Task Metrics.Executor Run Time"))
      .sum
    result +:= s"timeSuccesSuccesShuffleTasksExecutorRunTime : $timeSuccesSuccesShuffleTasksExecutorRunTime "

    val timeSuccesSuccesResultTasksExecutorRunTime = succedTasks.filter(getString(_, ".Task Type") == "ResultTask")
      .map(getBigDecimal(_, ".Task Metrics.Executor Run Time"))
      .sum
    result +:= s"timeSuccesSuccesResultTasksExecutorRunTime : $timeSuccesSuccesResultTasksExecutorRunTime "

    val numberSuccesShuffleRemoteBytesRead = succedTasks.filter(getString(_, ".Task Type") == "ShuffleMapTask")
      .map(getBigDecimal(_, ".Task Metrics.Shuffle Read Metrics.Remote Bytes Read", 0))
      .sum
    result +:= s"numberSuccesShuffleRemoteBytesRead : $numberSuccesShuffleRemoteBytesRead"

    val numberSuccesShuffleBytesWritten = succedTasks.filter(getString(_, ".Task Type") == "ShuffleMapTask")
      .map(getBigDecimal(_, ".Task Metrics.Shuffle Write Metrics.Shuffle Bytes Written", 0))
      .sum
    result +:= s"numberSuccesShuffleBytesWritten : $numberSuccesShuffleBytesWritten"

    val numberSuccesResultRemoteBytesRead = succedTasks.filter(getString(_, ".Task Type") == "ResultTask")
      .map(getBigDecimal(_, ".Task Metrics.Shuffle Read Metrics.Remote Bytes Read", 0))
      .sum
    result +:= s"numberSuccesResultRemoteBytesRead : $numberSuccesResultRemoteBytesRead"

    val numberSuccesResultBytesWritten = succedTasks.filter(getString(_, ".Task Type") == "ResultTask")
      .map(getBigDecimal(_, ".Task Metrics.Shuffle Write Metrics.Shuffle Bytes Written", 0))
      .sum
    result +:= s"numberSuccesResultBytesWritten : $numberSuccesResultBytesWritten"

    val numberSuccedBytesRead = succedTasks.map(getBigDecimal(_, ".Task Metrics.Input Metrics.Bytes Read", 0))
      .sum

    result +:= s"numberSuccedBytesRead: $numberSuccedBytesRead"

    val numberSuccedResultSize = succedTasks.map(getBigDecimal(_, ".Task Metrics.Result Size", 0)).sum
    result +:= s"numberSuccedResultSize: $numberSuccedResultSize"

    val timeSuccesExecutorDeserializeTime = succedTasks.map(getBigDecimal(_, ".Task Metrics.Executor Deserialize Time", 0)).sum
    result +:= s"timeSuccesExecutorDeserializeTime: $timeSuccesExecutorDeserializeTime"

    val timeSuccedShuffleWriteTime = succedTasks.map(getBigDecimal(_, ".Task Metrics.Shuffle Write Metrics.Shuffle Write Time", 0)).sum
    result +:= s"timeSuccedShuffleWriteTime: $timeSuccedShuffleWriteTime"

    val failedTasks = parsed.filter(x => getString(x, ".Task End Reason.Reason") != "Success")
    val numberFailedShuffleTasks = failedTasks.filter(getString(_, ".Task Type") == "ShuffleMapTask").length
    result +:= s"numberFailedShuffleTasks : $numberFailedShuffleTasks "
    val numberFailedResultTasks = failedTasks.filter(getString(_, ".Task Type") == "ResultTask").length
    result +:= s"numberFailedResultTasks : $numberFailedResultTasks "

    val timeFailedShuffleTasksExecutorRunTime = failedTasks.filter(getString(_, ".Task Type") == "ShuffleMapTask")
      .map(getBigDecimal(_, ".Task Metrics.Executor Run Time"))
      .sum
    result +:= s"timeFailedSuccesShuffleTasksExecutorRunTime : $timeFailedShuffleTasksExecutorRunTime"

    val timeFailedResultTasksExecutorRunTime = failedTasks.filter(getString(_, ".Task Type") == "ResultTask")
      .map(getBigDecimal(_, ".Task Metrics.Executor Run Time"))
      .sum
    result +:= s"timeFailedSuccesResultTasksExecutorRunTime : $timeFailedResultTasksExecutorRunTime"

    val numberFailedShuffleRemoteBytesRead = failedTasks.filter(getString(_, ".Task Type") == "ShuffleMapTask")
      .map(getBigDecimal(_, ".Task Metrics.Shuffle Read Metrics.Remote Bytes Read", 0))
      .sum
    result +:= s"numberFailedShuffleRemoteBytesRead : $numberFailedShuffleRemoteBytesRead "

    val numberFailedShuffleBytesWritten = failedTasks.filter(getString(_, ".Task Type") == "ShuffleMapTask")
      .map(getBigDecimal(_, ".Task Metrics.Shuffle Write Metrics.Shuffle Bytes Written", 0))
      .sum
    result +:= s"numberFailedShuffleBytesWritten : $numberFailedShuffleBytesWritten"

    val numberFailedResultRemoteBytesRead = failedTasks.filter(getString(_, ".Task Type") == "ResultTask")
      .map(getBigDecimal(_, ".Task Metrics.Shuffle Read Metrics.Remote Bytes Read", 0))
      .sum
    result +:= s"numberFailedResultRemoteBytesRead : $numberFailedResultRemoteBytesRead"

    val numberFailedResultBytesWritten = failedTasks.filter(getString(_, ".Task Type") == "ResultTask")
      .map(getBigDecimal(_, ".Task Metrics.Shuffle Write Metrics.Shuffle Bytes Written", 0))
      .sum
    result +:= s"numberFailedResultBytesWritten : $numberFailedResultBytesWritten"
    printf("")

    result.sortWith(_ < _) mkString ("\n")

  }

  val gnuplotTemplate = """|set style fill transparent solid 0.85
		  				   |set term pdf 
		  				   |set title "%s"
		  				   |set xlabel "Max Stage Id"
		  				   |set ylabel "ms or bytes"
		  				   |set output "tasks and stages.pdf"
		  				   |set style fill solid border -1
		  				   |plot  'stages' using 1:2 with lines lc 1 title  'Duration (ms)',   '' using 1:4 with lines lc 3 title 'Shuffled (bytes/10240)',  '' using 1:6 with lines lc 5 title 'Write time (ms/100)'""".stripMargin

  implicit def BigDecimalToString(bigDecimal: BigDecimal): String = {
    bigDecimal.toString
  }

  def getString(map: Map[String, JsValue], key: String, default: String = null): String = {
    require(map.get(key).isDefined || default != null, "There is no value with key: " + key)
    if (map.get(key).isDefined)
      map.get(key).get.asInstanceOf[JsString].value
    else default
  }

  def getBigDecimal(map: Map[String, JsValue], key: String, default: BigDecimal = null): BigDecimal = {
    require(map.get(key).isDefined || default != null, "There is no value with key: " + key)
    if (map.get(key).isDefined)
      map.get(key).get.asInstanceOf[JsNumber].value
    else default
  }

  //  def getBigDouble(map: Map[String, JsValue], key: String, default: BigDecimal = null): BigDecimal = {
  //    require(map.get(key).isDefined || default != null, "There is no value with key: " + key)
  //    if (map.get(key).isDefined)
  //      map.get(key).get.asInstanceOf[JsNumber].value
  //    else default
  //  }

  def mapMapsToUsefulArray(x: Map[String, spray.json.JsValue]): Array[String] = {
    Array[String](
      getBigDecimal(x, ".Stage ID"),
      getBigDecimal(x, ".Task Info.Task ID"),
      getBigDecimal(x, ".Task Info.Finish Time") - getBigDecimal(x, ".Task Info.Launch Time"),
      (getBigDecimal(x, ".Task Metrics.Shuffle Read Metrics.Remote Bytes Read", 0) +
        getBigDecimal(x, ".Task Metrics.Shuffle Write Metrics.Shuffle Bytes Written", 0)) / 1024)
  }

  def jsflatter(value: JsValue,
    accum: String = ""): Map[String, JsValue] = {
    value match {
      case _: JsArray => Map()
      case x: JsObject => {
        if (x.fields.isEmpty) {
          Map()
        } else {
          x.fields.map(field => jsflatter(field._2, accum + "." + field._1)).toList.reduce(_ ++ _)
          //          ms.tail.foldLeft(ms.head)(_ ++ _)
        }
      }
      case x => Map[String, JsValue](accum -> x)

    }
  }

  def createContentOfCSVFile(res: List[Map[String, spray.json.JsValue]]): String = {

    //    val ready = res.map(_.toString).mkString("\n")

    require(!res.isEmpty, "res is empty")
    val headers = res.map(m => m.keys.toSet)
      .reduce(_ ++ _)
      .toList
      .sorted

    val underHeaders = res.map(m => headers.map(header => m.getOrElse(header, "").toString))
    val bigcsv = (headers +: underHeaders).map(l => l.mkString(", ")).mkString("\n")
    bigcsv
  }

  def createFile(path: String, content: String): Unit = {
    val aout = new PrintWriter(new FileWriter(path));
    aout.print(content)
    aout.close
  }
}

