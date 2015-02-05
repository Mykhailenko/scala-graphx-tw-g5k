package util

import java.io.File
import java.io.PrintWriter
import java.io.FileWriter
import java.util.regex.Pattern

object RaportMaker {
  val metrics = Array("vertexCut", "largestPartition", "NSTDEV", "communicationCost", "replicationFactor", "balance")

  def main(args: Array[String]) {

    //doit(args(0))
    pezdoit(args(0))

  }
  def pezdoit(rootPath: String) {
    val root = new File(rootPath)
    val arrfiles = groupFiles(root)
    var str: String = ""

    for (files <- arrfiles)
      for (m <- metrics)
        str += zaebatoConfidenceInterval(files, m)

    val out = new PrintWriter(new FileWriter(root + "/report"));
    println("going to right string with " + str.length())
    
    out.print(str)
    out.close
  }

  def groupFiles(root: File): Array[Array[File]] = {
    root.listFiles().filter(f => f.isDirectory()).groupBy(f => smartSub(f.getName())).map(x => x._2).toArray
  }

  def smartSub(s: String): String = {
    var p = Pattern.compile("([^_0123456789]*)[_0123456789]*")
    var m = p.matcher(s)
    if (m.find()) {
      m.group(1)
    } else {
      " "
    }
  }

  def zaebatoConfidenceInterval(graphs: Array[File], metricName: String): String = {

//    for (g <- graphs) println(g.getName())
    var ar = Array[CSVChartData]()
    for (graph <- graphs) {
      ar = ar :+ pizdorez(graph, metricName)
    }

    var x1 = ar.map(x => x.m)
    var x2 = x1.reduce((a, b) => a ++ b)
    var x3 = x2.groupBy(x => x._1)
    var x4 = x3.mapValues(arr => arr.map(pair => pair._2))
    var x5 = x4.mapValues(aais => {
      aais.foldLeft(Map[Int, Array[Double]]())((a, b) => {
        var bb = b.map(pair => (pair._1, pair._2.toDouble))
        var aa = a
        for (p <- bb) {
          var res = aa.getOrElse(p._1, Array())
          res :+ p._2
          aa = aa + (p._1 -> res)
        }
        aa
      })
        .toArray
    });

    var folded = ar
      .map(x => x.m)
      .reduce((a, b) => a ++ b)
      .groupBy(x => x._1)
      .mapValues(arr => arr.map(pair => pair._2))
      .mapValues(aais => {
        var eba = aais.foldLeft(Map[Int, Array[Double]]())((a, b) => {
          var bb = b.map(pair => (pair._1, pair._2.toDouble))
          var aa = a
          for (p <- bb) {
            var res = aa.getOrElse(p._1, Array())
            res = res :+ p._2
            aa = aa + (p._1 -> res)
          }
          aa
        })
          .toArray
        
        eba.sortWith(_._1 < _._1)
      })
    var data = folded.toArray
    println("at least we have data " + data)
    var chart = ar(0)
    var result = "";
    val numberOfPartitioners = data.length
    val numberOfExperiments = data.head._2.length
    val numberOfVariants = data.head._2(0)._2.length
    val cells: Array[Array[String]] = Array
      .fill[Array[String]](numberOfExperiments + 3)(Array.fill[String](numberOfPartitioners * numberOfVariants + 1)(""))

    cells(0)(0) = chart.graphName + " " + metricName

    cells(1)(0) = "Number of partitions"
    for (j <- 0 until numberOfPartitioners) {
      cells(1)(1 + j * numberOfVariants) = data(j)._1
    }

    for (j <- 0 until numberOfPartitioners) {
      for (v <- 0 until numberOfVariants) {
        cells(2)(1 + j * numberOfVariants + v) = v.toString
      }
    }

    for (i <- 0 until numberOfExperiments) {
      cells(3 + i)(0) = data.head._2(i)._1.toString

      for (j <- 0 until numberOfPartitioners) {
        for (v <- 0 until numberOfVariants) {
          cells(3 + i)(1 + j * numberOfVariants + v) = data(j)._2(i)._2(v).toString
        }
      }
    }

    // fill cells
    for (i <- 0 until cells.length) {
      for (j <- 0 until cells.head.length - 1) {
        result += cells(i)(j) + " , ";

      }
      result += cells(i)(cells.head.length - 1) + "\n"
    }
    result
  }

  def doit(rootPath: String) {

    val root = new File(rootPath)
    var str: String = ""
    for (graph <- root.listFiles() if graph.isDirectory())
      for (m <- metrics)
        str += zaebato(graph, m)

    val out = new PrintWriter(new FileWriter(root + "/report"));
    out.print(str)
    out.close

  }

  def pizdorez(graph: File, metricName: String): CSVChartData = {
    val graphName = graph.getName()
    val data = new CSVChartData(graphName, metricName)
    for (p <- graph.listFiles() if p.isDirectory()) {
      val partitionName = p.getName()
      var arr = Array[(Int, String)]()
      for (cas <- p.listFiles() if cas.isFile() && cas.getName().endsWith(".json")) {
        val partitionNumber = cas.getName().substring(0, cas.getName().length() - ".json".length).toInt
        val metricValue = new JsonReport(cas.getAbsolutePath()).get(metricName)
        arr = arr :+ (partitionNumber, metricValue)
      }
      arr = arr.sortWith(_._1 < _._1)
      data.addPoints(partitionName, arr)
    }

    data
  }

  def zaebato(graph: File, metricName: String): String = {
    val graphName = graph.getName()
    val data = new CSVChartData(graphName, metricName)
    for (p <- graph.listFiles() if p.isDirectory()) {
      val partitionName = p.getName()
      var arr = Array[(Int, String)]()
      for (cas <- p.listFiles() if cas.isFile() && cas.getName().endsWith(".json")) {
        val partitionNumber = cas.getName().substring(0, cas.getName().length() - ".json".length).toInt
        val metricValue = new JsonReport(cas.getAbsolutePath()).get(metricName)
        arr = arr :+ (partitionNumber, metricValue)
      }
      arr = arr.sortWith(_._1 < _._1)
      data.addPoints(partitionName, arr)
    }

    data.csv
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

    res += ("average" -> aver)
    res += ("min" -> arr.toList.min)
    res += ("max" -> arr.toList.max)

    var percentale95 = 1.96 * stddev(arr) / Math.sqrt(arr.length)
    var percentale90 = 1.645 * stddev(arr) / Math.sqrt(arr.length)
    res += ("+95%" -> (aver + percentale95))
    res += ("-95%" -> (aver - percentale95))
    res += ("+90%" -> (aver + percentale90))
    res += ("-90%" -> (aver - percentale90))

    res
  }

}