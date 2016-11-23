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
import org.apache.spark.storage.StorageLevel
import java.util.Random
import org.apache.spark.graphx.lib.PageRank

object PageRankRunner {

  def main(args: Array[String]) {

    val pathToGrpah = args(0)
    val partitionerName = args(1)
    val pathToLogFile = args(2)
    val minEdgePartitions = args(3).toInt
    
    val log = new PrintWriter(new FileWriter(pathToLogFile));
    val startTime = System.currentTimeMillis()
    val nameOfGraph = pathToGrpah.substring(pathToGrpah.lastIndexOf("/") + 1)
    val sc = new SparkContext(new SparkConf()
      .setSparkHome(System.getenv("SPARK_HOME"))
      .setAppName(s" PartitionAndPageRankIterations $nameOfGraph $partitionerName $minEdgePartitions cores")
      .setJars(SparkContext.jarOfClass(this.getClass).toList))

    val t = System.currentTimeMillis()
    log.println("PartitionAndPageRankIterations")
    log.println("Context created " + (t - startTime) + " ms")
    log.flush()
    var graph: Graph[Int, Int] = null
    var conCom: VertexRDD[VertexId] = null
    graph = GraphLoader.edgeListFile(sc, pathToGrpah, false,
      edgeStorageLevel = StorageLevel.MEMORY_AND_DISK,
      vertexStorageLevel = StorageLevel.MEMORY_AND_DISK,
      minEdgePartitions = minEdgePartitions) //8 //0
    //graph.numVertices
    graph.edges.count // 1
    val t0 = System.currentTimeMillis()
    log.println("Graph loaded for " + (t0 - t) + " ms")
    log.flush()
    graph = graph.partitionBy(PartitionStrategy.fromString(partitionerName)) //.persist() // 9 // 2 // 10
    log.println("graph.edges.count = " + graph.edges.count) // 3
    val t1 = System.currentTimeMillis()
    log.println("Graph repartitioned " + (t1 - t0) + " ms")
    log.flush()
    val result = PageRank.run(graph, 10)
    log.println("result.edges.count = " + result.edges.count)
    val t2 = System.currentTimeMillis()
    log.println("10 Iterations of Page Rank has been calculated " + (t2 - t1) + " ms")
    log.flush()
    sc.stop
    log.close
  }
}
