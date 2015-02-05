package util

import java.io.File

import spray.json._
import DefaultJsonProtocol._
import java.io.PrintWriter
import java.io.FileWriter

object ProducteurDesReport {
  val metrics = Array("vertexCut", "largestPartition", "NSTDEV", "communicationCost", "replicationFactor", "balance")

  def main(args: Array[String]) {
    require(args.length == 2 || args.length == 3, "Wrong argument number. Should be 2. Usage: <path_to_root> <prefix> [<confidence>]")

    val root = args(0)
    val prefix = args(1)

    var data = ProducteurDesReport.getMaps(root).filter(mapa => mapa.getOrElse("graph", "").startsWith(prefix))
    var globalResult = ""
    for (metricName <- metrics) {
      val ns = getNs(data)

      val partitioners = getPartitioners(data)

      val graphs = getGraphs(data)

      if (args.length == 2) {
        var cells: Array[Array[String]] = Array
          .fill[Array[String]](ns.length + 3)(Array.fill[String](partitioners.length * graphs.length + 1)(""))
        cells = fillCells(cells, prefix, metricName, graphs, partitioners, ns, data)
        globalResult = makeCSVString(cells)
      } else if (args.length == 3) {
        var cells: Array[Array[String]] = Array
          .fill[Array[String]](ns.length + 3)(Array.fill[String](partitioners.length * 7 + 1)(""))
        cells = fillCellsConfidence(cells, prefix, metricName, graphs, partitioners, ns, data)
        globalResult = makeCSVString(cells)
      }
    }

    val out = new PrintWriter(new FileWriter(root + "/report" + prefix));

    out.print(globalResult)
    out.close

  }

  def getMaps(path: String): Array[Map[String, String]] = {
    var root = new File(path)
    var data = Array[Map[String, String]]()
    for (graph <- root.listFiles() if graph.isDirectory()) {
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
      cells(2)(1 + j * 7 + 4) = "+90"
      cells(2)(1 + j * 7 + 5) = "+95"
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
        cells(3 + i)(1 + j * 7 + 4) = st("+90").toString
        cells(3 + i)(1 + j * 7 + 5) = st("+95").toString
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
    var result = "";
    for (i <- 0 until cells.length) {
      for (j <- 0 until cells.head.length - 1) {
        result += cells(i)(j) + " , ";
      }
      result += cells(i)(cells.head.length - 1) + "\n"
    }
    result
  }

}


