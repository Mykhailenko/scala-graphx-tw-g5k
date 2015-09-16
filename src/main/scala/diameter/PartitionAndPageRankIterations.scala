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

object PartitionAndPageRankIterations {

  def main(args: Array[String]) {

    val pathToGrpah = args(0)
    val partitionerName = args(1)
    val filenameWithResult = args(2)
    val minEdgePartitions = args(3).toInt
    
    val out = new PrintWriter(new FileWriter(filenameWithResult));
    val startTime = System.currentTimeMillis()
    val nameOfGraph = pathToGrpah.substring(pathToGrpah.lastIndexOf("/") + 1)
    val sc = new SparkContext(new SparkConf()
      .setSparkHome(System.getenv("SPARK_HOME"))
      .setAppName(s" PartitionAndPageRankIterations $nameOfGraph $partitionerName $minEdgePartitions cores")
      .setJars(SparkContext.jarOfClass(this.getClass).toList))

    val t = System.currentTimeMillis()
    out.println("PartitionAndPageRankIterations")
    out.println("Context created " + (t - startTime) + " ms")
    out.flush()
    var graph: Graph[Int, Int] = null
    var conCom: VertexRDD[VertexId] = null
    graph = GraphLoader.edgeListFile(sc, pathToGrpah, false,
      edgeStorageLevel = StorageLevel.MEMORY_AND_DISK,
      vertexStorageLevel = StorageLevel.MEMORY_AND_DISK,
      minEdgePartitions = minEdgePartitions) //8 //0
    //graph.numVertices
    graph.edges.count // 1
    val t0 = System.currentTimeMillis()
    out.println("Graph loaded for " + (t0 - t) + " ms")
    out.flush()
    graph = graph.partitionBy(PartitionStrategy.fromString(partitionerName)) //.persist() // 9 // 2 // 10
    out.println("graph.edges.count = " + graph.edges.count) // 3
    val t1 = System.currentTimeMillis()
    out.println("Graph repartitioned " + (t1 - t0) + " ms")
    out.flush()
    val result = PageRank.run(graph, 10)
    out.println("result.edges.count = " + result.edges.count)
    val t2 = System.currentTimeMillis()
    out.println("10 Iterations of Page Rank has been calculated " + (t2 - t1) + " ms")
    out.flush()
    sc.stop
    out.close
  }
}
