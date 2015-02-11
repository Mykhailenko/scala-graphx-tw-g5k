package util

import java.io.File

import spray.json._
import DefaultJsonProtocol._
import scala.sys.process._
import java.io.PrintWriter
import java.io.FileWriter

object ProducteurDesReport {
  val metrics = Array("vertexCut", "largestPartition", "NSTDEV", "communicationCost", "replicationFactor", "balance", "Partitioning.time")

  def main(args: Array[String]) {
    require(args.length == 2, "Wrong argument number. Should be 2. Usage: <path_to_root> <prefix> ")

    val root = args(0)
    val prefix = args(1)

    val reportsRoot = new File(root + "/aggregated")
    reportsRoot.mkdir()
    val reportsRootAll = new File(reportsRoot.getAbsolutePath() + "/all")
    val reportsRootConfidence = new File(reportsRoot.getAbsolutePath() + "/confidence")
    reportsRootAll.mkdir()
    reportsRootConfidence.mkdir()

    var data = ProducteurDesReport.getMaps(root).filter(mapa => mapa.getOrElse("graph", "").startsWith(prefix))

    var globalResult = ""
    var globalResultConfidence = ""
    for (metricName <- metrics) {
      val ns = getNs(data)

      val partitioners = getPartitioners(data)

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
        tout = new PrintWriter(new FileWriter(reportsRootAll.getAbsolutePath() + "/" + g + metricName + ".gnu"));
        tout.println("set style fill transparent solid 0.85");
        tout.println(s"""set title "different partitions/metric==$metricName/graph==$g" """)
        tout.println("set term png");
        tout.println(s"""set output "$g$metricName.png"""");
        var s = s"""plot '$prefix$metricName.data' """;
        for (p <- 0 until partitioners.length) {
          if (p != 0) {
            s += ", '' "
          }
          val column = (2 + graphs.length * p + i)
          val partitionerName = partitioners(p)
          s += s""" using 1:$column with lines title '$partitionerName'  """
        }
        tout.println(s)
        tout.close
      }

      cells = Array
        .fill[Array[String]](ns.length + 3)(Array.fill[String](partitioners.length * 7 + 1)(""))
      cells = fillCellsConfidence(cells, prefix, metricName, graphs, partitioners, ns, data)
      globalResultConfidence += makeCSVString(cells)

      tout = new PrintWriter(new FileWriter(reportsRootConfidence.getAbsolutePath() + "/" + prefix + metricName + ".data"));
      tout.print(makeStirng(cells, "\t", 3, "#"))
      tout.close

      tout = new PrintWriter(new FileWriter(reportsRootConfidence.getAbsolutePath() + "/" + prefix + metricName + ".gnu"));
      tout.println("set style fill transparent solid 0.85");
      tout.println(s"""set title "95% confidence/metric==$metricName/graph==$prefix" """)
      tout.println("set term png");
      tout.println(s"""set output "$prefix$metricName.png"""");
      var s = s"""plot '$prefix$metricName.data' """;

      for (p <- 0 until partitioners.length) {
        if (p != 0) {
          s += ", '' "
        }
        val columnMinus95 = (1 + 7 * p + 3)
        val columnPlus95 = (1 + 7 * p + 5)
        val partitionerName = partitioners(p)
        s += s""" using 1:$columnMinus95:$columnPlus95 with filledcurves title '$partitionerName'  """
        createCandleChart(reportsRootConfidence.getAbsolutePath(), prefix, metricName, partitionerName, 1 + 7 * p + 4)
      }
      tout.println(s)
      tout.close

      //      Process("cd " + reportsRootConfidence.getAbsolutePath() + "/ ; gnuplot " + reportsRootConfidence.getAbsolutePath() + "/" + prefix + metricName + ".gnu ").run()

    }
    var script = new PrintWriter(new FileWriter(reportsRootConfidence.getAbsolutePath() + "/allrun.sh"));
    script.println("""|#!/usr/bin/bash
       |for entry in ./*gnu
       |do
       | gnuplot $entry
       |done""".stripMargin)

    script.close

    script = new PrintWriter(new FileWriter(reportsRootAll.getAbsolutePath() + "/allrun.sh"));
    script.println("""|#!/usr/bin/bash
       |for entry in ./*gnu
       |do
       | gnuplot $entry
       |done""".stripMargin)

    script.close

    val out = new PrintWriter(new FileWriter(reportsRoot + "/report" + prefix));
    out.print(globalResult)
    out.close

    val outC = new PrintWriter(new FileWriter(reportsRoot + "/report" + prefix + "Confidence"));
    outC.print(globalResultConfidence)
    outC.close
  }

  def createCandleChart(root: String, prefix: String, metricName: String, partitionerName: String, mean: Int): Unit = {
    val tout = new PrintWriter(new FileWriter(root + "/" + prefix + metricName + partitionerName + ".gnu"));
    tout.println("set style fill transparent solid 0.85");
    tout.println(s"""set title "95% confidence/metric==$metricName/graph==$prefix/partition=$partitionerName" """)
    tout.println("set term png");
    tout.println(s"""set output "$prefix$metricName$partitionerName.png"""");
    val open = mean - 1
    val low = mean - 3
    val high = mean + 3
    val close = mean + 1

    tout.println(s"""plot '$prefix$metricName.data' using 1:$open:$low:$high:$close with candlesticks title '$partitionerName' """);
    tout.close

  }

  def getMaps(path: String): Array[Map[String, String]] = {
    var root = new File(path)
    var data = Array[Map[String, String]]()
    for (graph <- root.listFiles() if graph.isDirectory() && graph.getName() != "aggregated") {
      for (partitioner <- graph.listFiles() if partitioner.isDirectory()) {
        for (json <- partitioner.listFiles() if json.isFile() && json.getName().endsWith(".json")) {
          val partitionNumber = json.getName().substring(0, json.getName().length() - ".json".length).toInt

          var mapa = scala.io.Source.fromFile(json.getAbsolutePath()).mkString.parseJson.convertTo[Map[String, String]]
          mapa += ("graph" -> graph.getName())
          mapa += ("partitionerName" -> partitioner.getName())
          mapa += ("N" -> partitionNumber.toString)
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
          cells(3 + i)(1 + j * graphs.length + g) = data.filter(mapa => {
            (mapa.getOrElse("graph", "") == graphs(g)) && (mapa.getOrElse("partitionerName", "") == partitioners(j)) && (mapa.getOrElse("N", "").toInt == ns(i))
          }).head.getOrElse(metricName, "")
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


