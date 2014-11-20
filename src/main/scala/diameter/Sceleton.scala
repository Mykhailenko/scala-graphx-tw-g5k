package diameter

import util.JsonLogger
import org.apache.spark.SparkContext._
import org.apache.spark._
import org.apache.spark.graphx._
import org.apache.spark.rdd.RDD
import scala.collection.immutable.Vector
import sun.security.provider.certpath.Vertex
import java.io.File
import java.io.PrintWriter
import java.io.FileWriter

object Sceleton {

  def main(args: Array[String]) {
    require(args.length == 1, "Wrong argument number. Should be 1. Usage: <path_to_grpah> ")

    val sc = new SparkContext(new SparkConf()
      .setSparkHome(System.getenv("SPARK_HOME"))
      .setAppName("app's awesome name")
      .setJars(SparkContext.jarOfClass(this.getClass).toList))

//    val file = new File(args(1));
//    val out = new PrintWriter(new FileWriter(file));

    var graph: Graph[Int, Int] = null
    var result : Array[(VertexId, Double)] = null

    JsonLogger(sc) { logger =>
      import logger._

      logGraphLoading {
        Thread.sleep(1000)
      }

      logPartitioning{
        Thread.sleep(1000)
      }
      logCalculationAfterPartitioning(graph)
      
      logAlgorithExecution {
        Thread.sleep(1488)
      }

      logResultSaving{
        Thread.sleep(1589)
      }
    }

  }
}
