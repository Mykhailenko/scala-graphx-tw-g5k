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

object ErdosReniy2 {
  def main(args: Array[String]) {
    require(args.length == 4, "[number of vertices] [number of edges] [path to grpah] [T]")

    val V = args(0).toInt
    val E = args(1).toLong
    val path = args(2)
    val T = args(3).toInt
//    val out = new PrintWriter(new BufferedWriter(new FileWriter(path)));

    val sc = new SparkContext(new SparkConf()
      .setSparkHome(System.getenv("SPARK_HOME"))
      .setJars(SparkContext.jarOfClass(this.getClass).toList))

    val part = E / T
    val ar = for (i <- 0 until T) yield part.toInt

    val result = sc.parallelize(ar, T).flatMap(x => {
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
    .map(x => {
      x._1 + " " + x._2
    })
    
    
    
    result.coalesce(1).saveAsTextFile(path)
    
    println("result.count = " + result.count )
    
    if(result.count < E){
      println("But we need more " + (E - result.count))  
    }


  }

}


