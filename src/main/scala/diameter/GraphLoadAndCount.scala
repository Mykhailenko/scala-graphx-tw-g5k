package diameter

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

object GraphLoadAndCount {

  def main(args: Array[String]) {

    require(args.length >= 2, s"""Wrong argument number. 
                                  	|Usage: <path_to_grpah> <minEdgePartitions>""".stripMargin)

    var graph: Graph[Int, Int] = null
    val nameOfGraph = args(0).substring(args(0).lastIndexOf("/") + 1)
    val minEdgePartitions = args(1).toInt

    val sc = new SparkContext(new SparkConf()
      .setSparkHome(System.getenv("SPARK_HOME"))
      .setAppName(s" load and count() graph: $nameOfGraph ")
      .setJars(SparkContext.jarOfClass(this.getClass).toList))

    graph = GraphLoader.edgeListFile(sc, args(0), false, minEdgePartitions = minEdgePartitions)
    

    if (args.length == 3) {
      val partitionerName = args(2)
      graph = graph.partitionBy(PartitionStrategy.fromString(partitionerName))
    }
    
    println("after repartitioning")
    
    for((p,ep) <- graph.edges.partitionsRDD.collect){
      println(s"In partition: $p")
      for(i <- 0 until ep.srcIds.length){
        println("\t" + ep.srcIds.apply(i) + " -> " + ep.dstIds.apply(i))
      }
    }

    graph = graph.partitionBy(PartitionStrategy.RandomVertexCut)
    
    for((p,ep) <- graph.edges.partitionsRDD.collect){
      println(s"In partition: $p")
      for(i <- 0 until ep.srcIds.length){
        println("\t" + ep.srcIds.apply(i) + " -> " + ep.dstIds.apply(i))
      }
    }
    
    println("\n\n\n" + graph.edges.count + "\n\n\n")
    println("\n\n\n" + graph.vertices.count + "\n\n\n")
  }
}