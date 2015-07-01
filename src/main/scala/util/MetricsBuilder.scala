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

  var metrics = Set[String]()

  var eventLogFilter = fileParses(parseEventLogFile, getMetainfoFromEventLogFile, listOfFilesEventLog) _

  def addMetaMetric(metric: String): Unit = {
    metrics += metric.replaceAll("\\s+", "").replaceAll("\\.", "");
  }

  def addHeader(header: String): Unit = {
    val h = header.replaceAll("\\s+", "").replaceAll("\\.", "");
    if (!headers.contains(h)) {
      headers :+= h
    }
  }

  def countParsing(rootFolder: File,
    graph: String,
    version: String,
    partitioner: String,
    cores: String,
    metric: String): Int = {
    var res = Map[Int, Int]()
    val files = listOfFilesEventLog(rootFolder)
    for (file <- files) {
      val eventLogFile = file
      val meta = getMetainfoFromEventLogFile(file)
      if (getString(meta, ".graph").toLowerCase().matches(graph) &&
        getString(meta, ".version").toLowerCase().matches(version) &&
        getString(meta, ".partitioner").toLowerCase().matches(partitioner) &&
        getBigDecimal(meta, ".cores").toLowerCase().matches(cores)) {
        println("work with " + file.getName())
        val flatted = parseEventLogFile(eventLogFile)
        val filtered = flatted.filter(x => x.get(metric).isDefined )
        val mapped = filtered.map(getBigDecimal(_, metric).toInt)
        					 .filter(_ > 1000)
        
        val map = mapped.groupBy(x => x)
          .toList
          .map(x => (x._1.toInt, x._2.length))
        map.foreach(x => {
          if(res.get(x._1).isDefined){
            val v = res.get(x._1).get
            res += (x._1 -> (x._2 + v))
          }else{
            res += x
          } 
        })
      }
    }
    res.toList.map(_._2).reduce(_ + _ )
  }
  def cumulativeParsing(rootFolder: File,
    graph: String,
    version: String,
    partitioner: String,
    cores: String,
    metric: String): List[(Int, Double)] = {
    var res = Map[Int, Int]()
    val files = listOfFilesEventLog(rootFolder)
    for (file <- files) {
      val eventLogFile = file
      val meta = getMetainfoFromEventLogFile(file)
      if (getString(meta, ".graph").toLowerCase().matches(graph) &&
        getString(meta, ".version").toLowerCase().matches(version) &&
        getString(meta, ".partitioner").toLowerCase().matches(partitioner) &&
        getBigDecimal(meta, ".cores").toLowerCase().matches(cores)) {
        println("work with " + file.getName())
        val flatted = parseEventLogFile(eventLogFile)
        val filtered = flatted.filter(x => x.get(metric).isDefined )
        val mapped = filtered.map(getBigDecimal(_, metric).toInt)
//        					 .filter(_ > 1000)
        
        val map = mapped.groupBy(x => x)
          .toList
          .map(x => (x._1.toInt, x._2.length))
        map.foreach(x => {
          if(res.get(x._1).isDefined){
            val v = res.get(x._1).get
            res += (x._1 -> (x._2 + v))
          }else{
            res += x
          } 
        })
      }
    }
    cumulativeDistribution(res.toList.sortWith((a, b) => a._1 < b._1))
  }

  //  val s = fileParses(List[File]())
  def fileParses(
    fileParser: (File => List[Map[String, JsValue]]),
    metadataFileParser: (File => Map[String, JsValue]),
    listOfFile: (File => List[File]))(rootFolder: File,
      graph: String = ".*",
      version: String = ".*",
      partitioner: String = ".*",
      cores: String = ".*"): List[Map[String, JsValue]] = {
    var res = List[Map[String, JsValue]]()
    val files = listOfFile(rootFolder)
    for (file <- files) {
      val eventLogFile = file
      val meta = metadataFileParser(file)
      if (getString(meta, ".graph").toLowerCase().matches(graph) &&
        getString(meta, ".version").toLowerCase().matches(version) &&
        getString(meta, ".partitioner").toLowerCase().matches(partitioner) &&
        getBigDecimal(meta, ".cores").toLowerCase().matches(cores)) {
        val flatted = fileParser(eventLogFile).map(_ ++ meta)
        println(file.getName() + " added")
        res ++= flatted

      }

    }
    res
  }
  /**
   *   "-partitionandpagerank-twitter_1.txt-edgepartition1d-7-cores-1434447778082"
   * pagerank - algorithm
   * twitter - graph
   * 1 - version
   * edgepartition1d - partitioner
   * 7 - cores
   */

  def getMetainfoFromEventLogFile(file: File): Map[String, JsValue] = {
    val parentFolderName = file.getParentFile().getName()
    println("parentFolderName = " + parentFolderName)
    val splitted = parentFolderName.split("-")
    println(splitted(1))
    val algorithm = splitted(1).substring("partitionand".length)
    val subsplitted = splitted(2).split("_")
    val graph = subsplitted(0)
    val version = subsplitted(1).replaceAll("[^\\d]", "")
    val partitioner = splitted(3)
    val cores = splitted(4)

    var res = Map[String, JsValue]()
    res += (".algorithm" -> JsString(algorithm))
    res += (".graph" -> JsString(graph))
    res += (".version" -> JsString(version))
    res += (".partitioner" -> JsString(partitioner))
    res += (".cores" -> JsNumber(cores))
    res
  }

  def parseEventLogFile(file: File): List[Map[String, JsValue]] = {
    Source.fromFile(file)
      .getLines()
      .map(_.parseJson)
      .map(jsflatter(_))
      .toList
  }

  /**
   *   "/twitter_1/edgepartition1d/7.json"
   * twitter - graph
   * 1 - version
   * edgepartition1d - partitioner
   * 7 - cores
   */

  def getMetainfoFromJsonMetricFile(file: File): Map[String, JsValue] = {

    val parentFolder = file.getParentFile()

    val parentParentFolder = parentFolder.getParentFile()

    val splitted = parentParentFolder.getName().split("_")

    val graph = splitted(0)
    val version = splitted(1)
    val partitioner = parentFolder.getName()
    val cores = file.getName().split(".")(0)

    var res = Map[String, JsValue]()
    res += (".graph" -> JsString(graph))
    res += (".version" -> JsString(version))
    res += (".partitioner" -> JsString(partitioner))
    res += (".cores" -> JsNumber(cores))
    res
  }

  def parseJsonMetricFile(file: File): List[Map[String, JsValue]] = {
    List(jsflatter(Source.fromFile(file).mkString.parseJson))
  }

  def listFiles(folder: File): List[File] = {
    require(folder.isDirectory())

    var res = List[File]()
    for (f <- folder.listFiles()) {
      if (f.isFile()) {
        res :+= f
      } else {
        res ++= listFiles(f)
      }
    }
    res
  }

  def listOfFilesEventLog(folder: File): List[File] = {
    var res = List[File]()
    val eventLogFolder = new File(folder.getAbsolutePath() + "/eventLog")
    println(folder.getAbsolutePath() + "/eventLog")
    require(eventLogFolder.isDirectory())

    for (file <- listFiles(eventLogFolder) if file.getName() == "EVENT_LOG_1") {
      require(file.isFile())

      res :+= file
    }

    res
  }

  def listOfFilesJsonMetricFile(folder: File): List[File] = {
    var res = List[File]()

    require(folder.isDirectory())

    for (
      subFolder <- folder.listFiles() if subFolder.isDirectory() &&
        subFolder.getName().indexOf("_") != -1
    ) {
      require(subFolder.isDirectory())

      for (
        file <- listFiles(subFolder) if file.getName().indexOf("json") != -1
      ) {
        res :+= file
      }

    }

    res
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
          addMetaMetric(metricName)
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
        addMetaMetric("Latency")
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
        addMetaMetric("Stage duration")
        line ++= stats(stagesDurationTrue).map(_.toInt.toString)
        result :+= line.mkString(", ")
      }
      val headersZipped = headers.zipWithIndex.map(x => x._2 + " : " + x._1)
      val content = "#" + headersZipped.mkString(", ") + "\n" + result.mkString("\n")
      val out = new PrintWriter(new FileWriter(resultPath + partitioner + ".csv"));
      out.print(content)
      out.close
      for (m <- metrics) {
        val a = m + "average"
        //        println("a =" + a)
        val index = headers.indexOf(a)
        //        println("index =" + index)
        createPdf(m, resultPath + m + ".pdf", resultPath + partitioner + ".csv", index)
        //        createPdf(".Task Metrics.Executor Run Time", resultPath + "TaskMetricsExecutorRunTime.pdf", resultPath + partitioner + ".csv",
        //          10)
      }
    }

  }

  def createPdf(
    title: String,
    outputFileName: String,
    dataFileName: String,
    averageColumnIndex: Int,
    whatDoWeDraw: String = "Tasks",
    ylabel: String = "Time (ms)",
    xlabel: String = "Partition/core number"): Unit = {
    val script = getGnuplotScriptForCandlestick(title, outputFileName, dataFileName, averageColumnIndex, whatDoWeDraw, ylabel, xlabel)
    createExecuteAndRemoveGnuplotScript(script, outputFileName + "gnu")
  }

  def createExecuteAndRemoveGnuplotScript(script: String, path: String): Unit = {
    val out = new PrintWriter(new FileWriter(path));
    out.print(script)
    out.close

    import scala.sys.process._
    val comm = s"gnuplot $path"
    println("comm = " + comm)
    comm.!

    val f = new File(path)
    if (f.isFile) f.delete

  }

  def getGnuplotScriptForCandlestickOnlyConfidenceInterval(
    title: String,
    outputFileName: String,
    dataFileName: String,
    averageColumnIndex: Int,
    whatDoWeDraw: String = "Tasks",
    ylabel: String = "Time (ms)",
    xlabel: String = "Partition/core number"): String = {
    val opened = averageColumnIndex - 1
    val min = averageColumnIndex - 2
    val max = averageColumnIndex + 2
    val closed = averageColumnIndex + 1

    val template = s"""|set style fill transparent solid 0.85
				       |set term pdf 
				       |set title "$title"
				  	   |set xlabel "$xlabel"
				  	   |set ylabel "$ylabel"
				  	   |set output "$outputFileName"
				       |plot  '$dataFileName' using 1:$opened:$opened:$closed:$closed lw 0.5 with candlesticks title 'Stages'""".stripMargin
    template
  }

  def getGnuplotScriptForCandlestick(
    title: String,
    outputFileName: String,
    dataFileName: String,
    averageColumnIndex: Int,
    whatDoWeDraw: String = "Tasks",
    ylabel: String = "Time (ms)",
    xlabel: String = "Partition/core number"): String = {
    val opened = averageColumnIndex - 1
    val min = averageColumnIndex - 2
    val max = averageColumnIndex + 2
    val closed = averageColumnIndex + 1

    val template = s"""|set style fill transparent solid 0.85
				       |set term pdf 
				       |set title "$title"
				  	   |set xlabel "$xlabel"
				  	   |set ylabel "$ylabel"
				  	   |set output "$outputFileName"
				       |plot  '$dataFileName' using 1:$opened:$min:$max:$closed lw 0.5 with candlesticks title 'Stages'""".stripMargin
    template
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
      data.map(_ / data.length)
      val average = BigDecimal(data.map(_ / data.length).sum)
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