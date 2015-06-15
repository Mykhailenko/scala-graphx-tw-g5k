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

object NoIdea {

  def main(args: Array[String]) {

    require(args.length == 3, """|Wrong argument number.
                                 |Should be 3. Usage: <pathToGrpah> <filenameWithResult> <minEdgePartitions> """.stripMargin)

    val pathToGrpah = args(0)
    val filenameWithResult = args(1)
    val minEdgePartitions = args(2).toInt

    val nameOfGraph = pathToGrpah.substring(pathToGrpah.lastIndexOf("/") + 1)

    val sc = new SparkContext(new SparkConf()
      .setSparkHome(System.getenv("SPARK_HOME"))
      .setJars(SparkContext.jarOfClass(this.getClass).toList))

    var graph: Graph[Int, Int] = null

    JsonLogger(sc, filenameWithResult, "") { logger =>
      import logger._

      logGraphLoading {
        graph = GraphLoader.edgeListFile(sc, pathToGrpah, false, edgeStorageLevel = StorageLevel.MEMORY_AND_DISK,
          vertexStorageLevel = StorageLevel.MEMORY_AND_DISK, minEdgePartitions = minEdgePartitions)
      }

      logPartitioning {
        graph = graph.partitionBy(PartitionStrategy.RandomVertexCut)
        // just to make sure that partition really did
        graph.edges.partitionsRDD.collect
      }
      logCalculationAfterPartitioning(graph)

      logAlgorithExecution {
        graph = graph.mapVertices((id, value) => 0)
        graph = graph.mapEdges(edge => 1)
        val olderFollowers: VertexRDD[Int] = graph.mapReduceTriplets[Int](
          triplet => Iterator((triplet.dstId, triplet.attr)),
          (a, b) => a + b // Reduce Function
          )
        println("id, attribudte")
        olderFollowers.collect.foreach(a => println(a._1 + " " + a._2))  
      }
    }
  }
}