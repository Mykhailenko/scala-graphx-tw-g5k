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

object ErdosReniy3 {
  def main(args: Array[String]) {
    require(args.length == 5, "[path to graph] [number of vertices] [add edges] [path to grpah] [T]")

    val g = args(0)
    val V = args(1).toInt
    val E = args(2).toLong
    val path = args(3)
    val T = args(4).toInt
    val out = new PrintWriter(new BufferedWriter(new FileWriter(path)));

    val sc = new SparkContext(new SparkConf()
      .setSparkHome(System.getenv("SPARK_HOME"))
      .setJars(SparkContext.jarOfClass(this.getClass).toList))

    val lines = sc.textFile(g, T)
    val edges = lines.map(line => {
      val a = line.split("\\W+")
      require(a.length == 2)
      (a(0).toInt, a(1).toInt)
    })

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

    var addEdges = E
    while (addEdges > 0) {
      println(s"rest $addEdges")
      val pair = createPair
      if (notContains(pair)) {
        addedset += pair
        addEdges -= 1
      }
    }

    for (add <- addedset) {
      out.println(add._1 + " " + add._2)
    }
    out.close
    //    result.coalesce(1).saveAsTextFile(path)
    //
    //    println("result.count = " + result.count)
    //
    //    if (result.count < E) {
    //      println("But we need more " + (E - result.count))
    //    }

  }

}


