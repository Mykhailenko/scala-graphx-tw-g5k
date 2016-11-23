package util

import util.JsonLogger0
import org.apache.spark.SparkContext._
import org.apache.spark._
import org.apache.spark.graphx._
import org.apache.spark.rdd.RDD
import scala.collection.immutable.Vector
import sun.security.provider.certpath.Vertex
import java.io.File
import java.io.PrintWriter
import java.io.FileWriter
import org.apache.spark.storage.StorageLevel
import java.util.Random

object DegreeDistribution {

  def main(args: Array[String]) {

    val pathToGrpah = args(0)
    val filenameWithResult = args(1)

    val sc = new SparkContext(new SparkConf()
      .setSparkHome(System.getenv("SPARK_HOME"))
      .setJars(SparkContext.jarOfClass(this.getClass).toList))

    val graph = GraphLoader.edgeListFile(sc, pathToGrpah, false)

    val out = new PrintWriter(new FileWriter(filenameWithResult, true));

    val result = graph.degrees.collect.groupBy(_._2).map(a => (a._1, a._2.length)).toArray.sortWith(_._1 < _._1)
    for (pair <- result){
      out.println(pair._1 + ";" + pair._2)
    }
    out close

  }
}
