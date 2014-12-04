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

    require (args.length == 2, "Usage: <flatGraph> <objectGraph> ")

    val sc = new SparkContext(new SparkConf()
    		.setAppName("Flat")
    		.setJars(SparkContext.jarOfClass(this.getClass).toList))

    val graph = GraphLoader.edgeListFile(sc, args(0), true, edgeStorageLevel = StorageLevel.MEMORY_AND_DISK,
      vertexStorageLevel = StorageLevel.MEMORY_AND_DISK, minEdgePartitions = 240).cache()

    graph.edges.saveAsObjectFile(args(1))
  }
}