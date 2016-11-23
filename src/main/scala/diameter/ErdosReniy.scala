package diameter

import java.io.PrintWriter
import java.io.FileWriter
import java.io.BufferedWriter
import java.util.Queue
import java.util.concurrent._
import scala.collection.mutable.MutableList
import scala.util
import org.apache.spark.SparkContext
import org.apache.spark.SparkConf

object ErdosReniy {
  def main(args: Array[String]) {
    require(args.length == 4, "[number of vertices] [number of edges] [path to er-grpah] [number of cores]")

    val V = args(0).toInt
    val E = args(1).toLong
    val path = args(2)
    val T = args(3).toInt

    val sc = new SparkContext(new SparkConf()
      .setSparkHome(System.getenv("SPARK_HOME"))
      .setJars(SparkContext.jarOfClass(this.getClass).toList))
    
    val part = E / T
    val ar = for (i <- 0 until T) yield part.toInt

    val edges = sc.parallelize(ar, T).flatMap(x => {
      val random = scala.util.Random
      for (e <- 0 until x) yield {
        var sourceId = 0
        var destinationId = 0
        while (sourceId == destinationId) {
          sourceId = random.nextInt(V)
          destinationId = random.nextInt(V)
        }
        (sourceId, destinationId)
      }
    }).distinct

    val dE = E - edges.count
    require(dE >= 0)
    val random = scala.util.Random
    var addedset = Set[(Int, Int)]()

    def notContains(pair: (Int, Int)): Boolean = {
      !addedset.contains(pair) && !rddcontains(pair)
    }
    def rddcontains(pair: (Int, Int)): Boolean = {
      edges.filter(x => x._1 == pair._1 && x._2 == pair._2).count > 0
    }
    def createPair: (Int, Int) = {
      var sourceId = 0
      var destinationId = 0
      while (sourceId == destinationId) {
        sourceId = random.nextInt(V)
        destinationId = random.nextInt(V)
      }
      (sourceId, destinationId)
    }

    var addEdges = dE.toInt
    while (addEdges > 0) {
      println(addEdges)
      val pairs = (for (e <- 0 until addEdges) yield createPair).toSet.toArray

      val existed = edges.filter(x => {
        pairs.filter(p => p._1 == x._1 && p._2 == x._2).size > 0
      }).toArray.toSet

      for (p <- pairs) {
        if (!addedset.contains(p) && !existed.contains(p)) {
          addedset += p
          addEdges -= 1
        }
      }
      println(s"rest $addEdges")
    }

    val result = edges.union(sc.parallelize(addedset.toList))
    require(result.count == E)

    result.map(x => x._1 + " " + x._2).coalesce(1).saveAsTextFile(path)

  }

}
