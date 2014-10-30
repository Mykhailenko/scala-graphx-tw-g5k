package diameter

import org.apache.spark.storage._
import org.apache.spark.storage._

import org.apache.spark.SparkContext._
import org.apache.spark._
import org.apache.spark.graphx._
import org.apache.spark.rdd.RDD
import java.io.File
import java.io.PrintWriter
import java.io.FileWriter

object SaveAsObject {
  def main(args: Array[String]) {

    if (args.length != 2) {
      System.err.println(
        "Usage: <flatGraph> <objectGraph> ")
      System.exit(1)
    }

    val conf = new SparkConf()
      .setAppName("Flat")
      .setJars(SparkContext.jarOfClass(this.getClass).toList)

    val sc = new SparkContext(conf)

    val graph = GraphLoader.edgeListFile(sc, args(0), true, edgeStorageLevel = StorageLevel.MEMORY_AND_DISK,
      vertexStorageLevel = StorageLevel.MEMORY_AND_DISK, minEdgePartitions = 1000).cache()

    graph.edges.coalesce(1, true).saveAsObjectFile(args(1))
  }
}