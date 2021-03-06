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

    require (args.length <= 3, "Usage: <flatGraph> <objectGraph> [partitioner]")

    val sc = new SparkContext(new SparkConf()
    		.setAppName("Flat")
    		.setJars(SparkContext.jarOfClass(this.getClass).toList))

    var graph = GraphLoader.edgeListFile(sc, args(0), true, edgeStorageLevel = StorageLevel.MEMORY_AND_DISK,
      vertexStorageLevel = StorageLevel.MEMORY_AND_DISK, minEdgePartitions = 240).cache()

    if(args.length == 3){
      graph = graph.partitionBy(PartitionStrategy.fromString(args(2)))
    }

    graph.edges.saveAsObjectFile(args(1))
  }
}
