package diameter

import util.JsonLogger
import org.apache.spark.SparkContext._
import org.apache.spark._
import org.apache.spark.graphx._
import org.apache.spark.rdd.RDD
import java.io.File
import java.io.PrintWriter
import java.io.FileWriter
import scala.util.Random

object Flatter {
  def main(args: Array[String]) {

    require(args.length >= 3, "Usage: <graph> <flatGraph> <numPartition> [how many edges use in percentage from 0.0 till 1.0]")

    val conf = new SparkConf()
      .setAppName("Flat")
      .setJars(SparkContext.jarOfClass(this.getClass).toList)

    val sc = new SparkContext(conf)

    var relationships: RDD[String] = null;
    JsonLogger(sc) { logger =>
      import logger._

      logGraphLoading {

        if (args.length == 4) {
          val percentage: Double = args(3).toDouble
          relationships = sc.textFile(args(0)).flatMap(line => {
            val fields = line.split(" ")
            val edges = fields.tail.tail.map(folowerId => folowerId.toLong + " " + fields.head.toLong)
            edges.filter(x => math.random < percentage)
          })
        } else {
          relationships = sc.textFile(args(0)).flatMap(line => {
            val fields = line.split(" ")
            fields.tail.tail.map(folowerId => folowerId.toLong + " " + fields.head.toLong)
          })
        }

      }
      logResultSaving {
        relationships.coalesce(1, true).saveAsTextFile(args(1))
      }
    }
  }
}