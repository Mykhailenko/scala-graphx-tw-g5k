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

    require(args.length >= 2, "Usage: <graph> <flatGraph> [how many edges use in percentage from 0.0 till 1.0]")

    val conf = new SparkConf()
      .setAppName("Flat")
      .setJars(SparkContext.jarOfClass(this.getClass).toList)

    val sc = new SparkContext(conf)

    var relationships: RDD[String] = null;
    JsonLogger(sc) { logger =>
      import logger._

      logGraphLoading {

        if (args.length == 3) {
          val percentage: Double = args(2).toDouble
          relationships = sc.textFile(args(0)).flatMap(line => {
            val fields = line.split(" ")
            val edges = fields.tail.tail.map(folowerId => folowerId + " " + fields.head)
            
            edges.filter(x => x.split("\\s+").length == 2)
                 .filter(x => math.random < percentage)
          })
        } else if (args.length == 2)  {
          relationships = sc.textFile(args(0)).flatMap(line => {
            val fields = line.split(" ")
            fields.tail.tail.map(folowerId => folowerId.toLong + " " + fields.head.toLong)
          })
        } else {
          println("NUmber of parameters should be 2 or 3")
          System.exit(1)
        }

      }
      logResultSaving {
        relationships.coalesce(1, true).saveAsTextFile(args(1))
      }
    }
  }
}
