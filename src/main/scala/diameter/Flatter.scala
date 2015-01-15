package diameter

import util.JsonLogger
import org.apache.spark.SparkContext._
import org.apache.spark._
import org.apache.spark.graphx._
import org.apache.spark.rdd.RDD
import java.io.File
import java.io.PrintWriter
import java.io.FileWriter

object Flatter {
  def main(args: Array[String]) {

    require(args.length == 2, "Usage: <graph> <flatGraph> ")

    val conf = new SparkConf()
      .setAppName("Flat")
      .setJars(SparkContext.jarOfClass(this.getClass).toList)

    val sc = new SparkContext(conf)
    var relationships: RDD[String] = null;
    JsonLogger(sc) { logger =>
      import logger._

      logGraphLoading {

        relationships = sc.textFile(args(0)).flatMap(line => {
          val fields = line.split(" ")
          fields.tail.tail.map(folowerId => folowerId.toLong + " " + fields.head.toLong)
        })

      }
      logResultSaving {
        relationships.coalesce(1, true).saveAsTextFile(args(1))

      }
    }

  }
}