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

object LoadAsFlat {
  def main(args: Array[String]) {

    if (args.length != 2) {
      System.err.println(
        "Usage: <objectGraph> 'result.")
      System.exit(1)
    }

    val conf = new SparkConf()
      .setAppName("Flat")
      .setJars(SparkContext.jarOfClass(this.getClass).toList)

    val sc = new SparkContext(conf)

    val graph = GraphLoader.edgeListFile(sc, args(0), true, edgeStorageLevel = StorageLevel.MEMORY_AND_DISK,
      vertexStorageLevel = StorageLevel.MEMORY_AND_DISK, minEdgePartitions = 1000).cache()

    val file = new File(args(1));
    val out = new PrintWriter(new FileWriter(file));

    out.println("Ola senior")
    out.println("num edges " + graph.numEdges)
    out.println("num vertices " + graph.numVertices)
    println("azazaza");
    out.close();
  }
}