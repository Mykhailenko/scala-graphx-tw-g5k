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

object Sampler {
  def main(args: Array[String]) {

    require(args.length == 4, "[path to graph] [path to result] [number of samples] [log]".stripMargin)

    val pathToGrpah = args(0)
    val out = new PrintWriter(new FileWriter(new File(args(1))));
    val N = args(2).toInt
    val log = new PrintWriter(new FileWriter(new File(args(3))));

    val sc = new SparkContext(new SparkConf()
      .setSparkHome(System.getenv("SPARK_HOME"))
      .setJars(SparkContext.jarOfClass(this.getClass).toList))

    log.println("Context has been started")

    var graph: Graph[Int, Int] = GraphLoader.edgeListFile(sc, pathToGrpah, false, edgeStorageLevel = StorageLevel.MEMORY_AND_DISK,
      vertexStorageLevel = StorageLevel.MEMORY_AND_DISK)

    log.println("Graph has been loaded")

    val sample = graph.vertices.takeSample(false, N)

    val result = for (vertex <- sample) yield {
      log.println(vertex._1)
      out println bfs(graph, vertex._1)
    }

    log.close
    out.close

  }

  def bfs(graph: Graph[Int, Int], sourceId: VertexId): Long = {

    val initialGraph = graph.mapVertices((id, _) => if (id == sourceId) true else false)
    val bfs = initialGraph.pregel(false)(
      (id, dist, newDist) => true, // Vertex Program
      triplet => { // Send Message
        Iterator((triplet.dstId, true))
      },
      (a, b) => true // Merge Message
      )
    bfs.vertices.filter(pair => pair._2).count  
  }

}