package util

import java.io.File
import spray.json._
import DefaultJsonProtocol._
import scala.sys.process._
import java.io.PrintWriter
import java.io.FileWriter

object ProducteurDesReport {
  //val metrics = Array("vertexCut", "largestPartition", "NSTDEV", "communicationCost", "replicationFactor", "balance", "Partitioning.time", "AlgorithExecution.time")
  val metrics = Array("GraphLoading.time", "loadPartExecSave", "applicationTime.time", "partAndExec", "vertexCut", "NSTDEV", "replicationFactor", "balance", "Partitioning.time", "AlgorithExecution.time") /// "networkshuffle"
  def main(args: Array[String]) {
    require(args.length >= 2, "Wrong argument number. Should be 2. Usage: <path_to_root> <prefix> [newopt]")

    val newopt = args.length != 2

    val root = args(0)
    val prefix = args(1)

    val reportsRoot = new File(s"$root/aggregated")
    reportsRoot.mkdir()
    val reportsRootAll = new File(reportsRoot.getAbsolutePath() + "/all")
    val reportsRootConfidence = new File(reportsRoot.getAbsolutePath() + "/confidence")
    reportsRootAll.mkdir()
    reportsRootConfidence.mkdir()

    val rootDir = new File(root)

    require(rootDir.isDirectory(), s"It seems path: $root does not point to a directory")

    var data = ProducteurDesReport.getMaps(rootDir).filter(mapa => mapa.getOrElse("graph", "").startsWith(prefix))

    var globalResult = ""
    var globalResultConfidence = ""
    for (metricName <- metrics) {
      val ns = getNs(data)

      val partitioners = getPartitioners(data)
      assert(partitioners.length > 0)

      val graphs = getGraphs(data)

      var cells: Array[Array[String]] = Array
        .fill[Array[String]](ns.length + 3)(Array.fill[String](partitioners.length * graphs.length + 1)(""))
      cells = fillCells(cells, prefix, metricName, graphs, partitioners, ns, data)
      globalResult += makeCSVString(cells)

      var tout = new PrintWriter(new FileWriter(reportsRootAll.getAbsolutePath() + "/" + prefix + metricName + ".data"));
      tout.print(makeStirng(cells, "\t", 3, "#"))
      tout.close

      for (i <- 0 until graphs.length) {
        val g = graphs(i)

        val nubmerOfVerticesToBeCut = if (newopt)
          data.filter(mapa => mapa.getOrElse("graph", "") == g)(0).getOrElse("numberVerticesCanBeCut", "").toInt
        else 0

        var ss = "set style fill transparent solid 0.85\n";
        if (metricName == "largestPartition") {
          ss += "set logscale y\n"
        }
        if (metricName == "vertexCut" && newopt) {
          ss += """set format y "%g %%"""" + "\n"
        }
        //        ss += s"""set title "different partitions/metric==$metricName/graph==$g"\n"""
        ss += "set term pdf\n";
        ss += s"""set xlabel "Partition number"\n"""
        ss += s"""set output "$g$metricName.pdf"\n"""
        ss += s"""plot '$prefix$metricName.data' """;
        for (p <- 0 until partitioners.length) {
          if (p != 0) {
            ss += ", '' "
          }
          val original = (2 + graphs.length * p + i)

          val column =
            if (metricName == "vertexCut" && newopt) {
              // from 14 to ($14/187)
              "($" + original + "/" + (nubmerOfVerticesToBeCut / 100) + ")"
            } else {
              original
            }

          val partitionerName = partitioners(p)
          val interval = 2 + p % 5

          ss += s""" using 1:$column with linespoints lw 3 lc $p lt -1 pi +$interval pt $p ps 0.3 title '$partitionerName'  """
        }
        writeStringToFile(ss, reportsRootAll.getAbsolutePath() + "/" + g + metricName + ".gnu")
      }

      cells = Array
        .fill[Array[String]](ns.length + 3)(Array.fill[String](partitioners.length * 7 + 1)(""))
      cells = fillCellsConfidence(cells, prefix, metricName, graphs, partitioners, ns, data)
      globalResultConfidence += makeCSVString(cells)

      writeStringToFile(makeStirng(cells, "\t", 3, "#"), reportsRootConfidence.getAbsolutePath() + "/" + prefix + metricName + ".data")

      var ss = "set style fill transparent solid 0.85\n"
      var avg = "set style fill transparent solid 0.85\n"
      //      ss += s"""set title "95% confidence/metric==$metricName/graph==$prefix"\n"""
      ss += "set term pdf\n"
      avg += "set term pdf\n"

      ss += "set xlabel \"Partition number\"\n"
      avg += "set xlabel \"Partition number\"\n"

      if (metricName == "Partitioning.time" ||
        metricName == "AlgorithExecution.time" ||
        metricName == "partAndExec") {
        ss += "set ylabel \"Time (seconds)\"\n"
        avg += "set ylabel \"Time (seconds)\"\n"
      } else if (metricName == "balance") {
        ss += "set ylabel \"Balance coefficient\"\n"
        avg += "set ylabel \"Balance coefficient\"\n"
      } else if (metricName == "vertexCut") {
        ss += "set ylabel \"Vertices cut\"\n"
        avg += "set ylabel \"Vertices cut\"\n"
      } else if (metricName == "replicationFactor") {
        ss += "set ylabel \"Replication factor coefficient\"\n"
        avg += "set ylabel \"Replication factor coefficient\"\n"
      }

      ss += s"""set output "$prefix$metricName.pdf"\n"""
      avg += s"""set output "$prefix$metricName.average.pdf"\n"""

      ss += s"""plot '$prefix$metricName.data' """;
      avg += s"""plot '$prefix$metricName.data' """;

      for (p <- 0 until partitioners.length) {
        if (p != 0) {
          ss += ", '' "
          avg += ", '' "
        }
        val columnMinus95 = (1 + 7 * p + 3)
        val columnPlus95 = (1 + 7 * p + 5)

        val mean = 1 + 7 * p + 4
        val open = mean - 1
        val low = mean - 3
        val high = mean + 3
        val close = mean + 1

        val partitionerName = partitioners(p)
        val interval = 2 + p % 5
        //        ss += s""" using 1:$columnMinus95:$columnPlus95 with filledcurves title '$partitionerName'  """
        ss += s""" using 1:$open:$low:$high:$close with candlesticks title '$partitionerName'  """
        if (metricName == "Partitioning.time" ||
          metricName == "AlgorithExecution.time" ||
          metricName == "partAndExec") {
          val dollar = "$"
          avg += s""" using 1:$mean with linespoint lw 3 lc $p lt -1 pi +$interval pt $p ps 0.3  title '$partitionerName'  """
        } else {
          avg += s""" using 1:$mean with linespoint lw 3 lc $p lt -1 pi +$interval pt $p ps 0.3  title '$partitionerName'  """
        }
        createCandleChart(reportsRootConfidence.getAbsolutePath(), prefix, metricName, partitionerName, 1 + 7 * p + 4)
      }
      writeStringToFile(avg, reportsRootConfidence.getAbsolutePath() + "/" + prefix + metricName + "Average.gnu")
      writeStringToFile(ss, reportsRootConfidence.getAbsolutePath() + "/" + prefix + metricName + ".gnu")

      //      Process("cd " + reportsRootConfidence.getAbsolutePath() + "/ ; gnuplot " + reportsRootConfidence.getAbsolutePath() + "/" + prefix + metricName + ".gnu ").run()

    }

    addAllRunScriptToFolder(reportsRootConfidence.getAbsolutePath())
    addAllRunScriptToFolder(reportsRootAll.getAbsolutePath())

    writeStringToFile(globalResult, reportsRoot + "/report" + prefix)
    writeStringToFile(globalResultConfidence, reportsRoot + "/report" + prefix + "Confidence")
  }

  def writeStringToFile(content: String, filename: String): Unit = {
    val out = new PrintWriter(new FileWriter(filename));
    out.print(content)
    out.close
  }

  def addAllRunScriptToFolder(path: String): Unit = {
    val script = new PrintWriter(new FileWriter(path + "/allrun.sh"));
    script.println("""|#!/usr/bin/bash
                      |for entry in ./*gnu
                      |do
                      | gnuplot $entry
                      |done""".stripMargin)
    script.close
  }

  def createCandleChart(root: String, prefix: String, metricName: String, partitionerName: String, mean: Int): Unit = {
    val tout = new PrintWriter(new FileWriter(root + "/" + prefix + metricName + partitionerName + ".gnu"));
    tout.println("set style fill transparent solid 0.85");
    tout.println(s"""set title "95% confidence/metric==$metricName/graph==$prefix/partition=$partitionerName" """)
    tout.println("set term pdf");
    tout.println(s"""set output "$prefix$metricName$partitionerName.pdf"""");
    val open = mean - 1
    val low = mean - 3
    val high = mean + 3
    val close = mean + 1

    tout.println(s"""plot '$prefix$metricName.data' using 1:$open:$low:$high:$close with candlesticks title '$partitionerName' """);
    tout.close

  }

  def getMaps(root: File): Array[Map[String, String]] = {
    var data = Array[Map[String, String]]()
    for (graph <- root.listFiles() if graph.isDirectory() && graph.getName() != "aggregated") {
      for (partitioner <- graph.listFiles() if partitioner.isDirectory()) {
        for (json <- partitioner.listFiles() if json.isFile() && json.getName().endsWith(".json")) {
          val partitionNumber = json.getName().substring(0, json.getName().length() - ".json".length).toInt

          var mapa = scala.io.Source.fromFile(json.getAbsolutePath()).mkString.parseJson.convertTo[Map[String, String]]
          if(new File(json.getAbsolutePath() + "c").isFile()){
        	var inout = scala.io.Source.fromFile(json.getAbsolutePath()).mkString.parseJson.convertTo[Map[String, String]]
        	val sum = inout.getOrElse("input", "0").toInt + inout.getOrElse("output", "0").toInt
        	mapa += ("networkshuffle" -> sum.toString)
          }
          mapa += ("graph" -> graph.getName())
          mapa += ("partitionerName" -> partitioner.getName())
          mapa += ("N" -> partitionNumber.toString)
          val x = mapa.get("Partitioning.time").get.toInt + mapa.get("AlgorithExecution.time").get.toInt
          mapa += ("partAndExec" -> x.toString)
          val all = mapa.get("Partitioning.time").get.toInt + mapa.get("AlgorithExecution.time").get.toInt +
          mapa.get("ResultSaving.time").get.toInt + mapa.get("GraphLoading.time").get.toInt 
          mapa += ("loadPartExecSave" -> all.toString)
          
          data = data :+ mapa
        }
      }
    }
    data
  }

  def getNs(data: Array[Map[String, String]]): Array[Int] = {
    Set(data.map(mapa => mapa.getOrElse("N", "").toInt): _*).toArray.sortWith(_ < _)
  }

  def getPartitioners(data: Array[Map[String, String]]): Array[String] = {
    Set(data.map(mapa => mapa.getOrElse("partitionerName", "")): _*).toArray

  }

  def getGraphs(data: Array[Map[String, String]]): Array[String] = {
    Set(data.map(mapa => mapa.getOrElse("graph", "")): _*).toArray.sortWith((a, b) => if (a.length() == b.length()) a < b else a.length() < b.length())
  }

  def fillCellsConfidence(cells: Array[Array[String]], prefix: String, metricName: String, graphs: Array[String], partitioners: Array[String], ns: Array[Int], data: Array[Map[String, String]]): Array[Array[String]] = {
    cells(0)(0) = prefix + " " + metricName

    cells(1)(0) = "Number of partitions"
    for (j <- 0 until partitioners.length) {
      cells(1)(1 + j * 7) = partitioners(j)
    }

    for (j <- 0 until partitioners.length) {
      cells(2)(1 + j * 7 + 0) = "min"
      cells(2)(1 + j * 7 + 1) = "-90"
      cells(2)(1 + j * 7 + 2) = "-95"
      cells(2)(1 + j * 7 + 3) = "mean"
      cells(2)(1 + j * 7 + 4) = "+95"
      cells(2)(1 + j * 7 + 5) = "+90"
      cells(2)(1 + j * 7 + 6) = "max"
    }

    for (i <- 0 until ns.length) {
      cells(3 + i)(0) = ns(i).toString

      for (j <- 0 until partitioners.length) {
        var d = data.filter(mapa => {
          (mapa.getOrElse("partitionerName", "") == partitioners(j)) && (mapa.getOrElse("N", "").toInt == ns(i))
        }).map(mapa => mapa.getOrElse(metricName, "").toDouble).toArray
        
        if (metricName == "Partitioning.time" ||
              metricName == "AlgorithExecution.time" ||
              metricName == "partAndExec") {
          d = d.map(x => x / 1000)
        }
        
        if (d.isEmpty) {
          for (q <- 0 to 6) {
            cells(3 + i)(1 + j * 7 + q) = "?"
          }
        } else {
          val st = stat(d)
          cells(3 + i)(1 + j * 7 + 0) = st("min").toString
          cells(3 + i)(1 + j * 7 + 1) = st("-90").toString
          cells(3 + i)(1 + j * 7 + 2) = st("-95").toString
          cells(3 + i)(1 + j * 7 + 3) = st("mean").toString
          cells(3 + i)(1 + j * 7 + 4) = st("+95").toString
          cells(3 + i)(1 + j * 7 + 5) = st("+90").toString
          cells(3 + i)(1 + j * 7 + 6) = st("max").toString
        }
      }
    }
    cells
  }

  def average(arr: Array[Double]): Double = {
    arr.toList.sum / arr.length
  }

  def stddev(arr: Array[Double]): Double = {
    var aver = average(arr)
    Math.sqrt(
      arr.toList.map(x => Math.pow(x - aver, 2)).sum / arr.length)
  }
  def stat(arr: Array[Double]): Map[String, Double] = {
    var res = Map[String, Double]()
    var aver = average(arr)

    res += ("mean" -> aver)
    res += ("min" -> arr.toList.min)
    res += ("max" -> arr.toList.max)

    var percentale95 = 1.96 * stddev(arr) / Math.sqrt(arr.length)
    var percentale90 = 1.645 * stddev(arr) / Math.sqrt(arr.length)
    res += ("+95" -> (aver + percentale95))
    res += ("-95" -> (aver - percentale95))
    res += ("+90" -> (aver + percentale90))
    res += ("-90" -> (aver - percentale90))

    res
  }

  def fillCells(cells: Array[Array[String]], prefix: String, metricName: String, graphs: Array[String], partitioners: Array[String], ns: Array[Int], data: Array[Map[String, String]]): Array[Array[String]] = {
    cells(0)(0) = prefix + " " + metricName

    cells(1)(0) = "Number of partitions"
    for (j <- 0 until partitioners.length) {
      cells(1)(1 + j * graphs.length) = partitioners(j)
    }

    for (j <- 0 until partitioners.length) {
      for (g <- 0 until graphs.length) {
        cells(2)(1 + j * graphs.length + g) = graphs(g)
      }
    }

    for (i <- 0 until ns.length) {
      cells(3 + i)(0) = ns(i).toString

      for (j <- 0 until partitioners.length) {
        for (g <- 0 until graphs.length) {
          val filtered = data.filter(mapa => {
            (mapa.getOrElse("graph", "") == graphs(g)) && (mapa.getOrElse("partitionerName", "") == partitioners(j)) && (mapa.getOrElse("N", "").toInt == ns(i))
          })
          if (filtered.isEmpty) {
            cells(3 + i)(1 + j * graphs.length + g) = "?"
          } else {
            cells(3 + i)(1 + j * graphs.length + g) = filtered.head.getOrElse(metricName, "")
          }
        }
      }
    }
    cells
  }

  def makeCSVString(cells: Array[Array[String]]): String = {
    makeStirng(cells, " , ", 0, "")
  }

  def makeStirng(cells: Array[Array[String]], delimiter: String, commented: Int, signToComment: String): String = {
    var result = "";
    for (i <- 0 until cells.length) {
      for (j <- 0 until cells.head.length - 1) {
        if (i < commented) {
          result += signToComment
        }
        result += cells(i)(j) + delimiter;
      }
      result += cells(i)(cells.head.length - 1) + "\n"
    }
    result
  }
}


