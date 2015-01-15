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

object PartitionerTester {

  def main(args: Array[String]) {

    require(args.length == 4, "Wrong argument number. Should be 3. Usage: <path_to_grpah> <partiotioner_name> <filename_with_result> <minEdgePartitions>")

    val sc = new SparkContext(new SparkConf()
      .setSparkHome(System.getenv("SPARK_HOME"))
      .setJars(SparkContext.jarOfClass(this.getClass).toList))

    var graph: Graph[Int, Int] = null
    val partitionerName = args(1)
    val minEdgePartitions = args(3).toInt
    
    JsonLogger(sc, args(2)) { logger =>
      import logger._

      logGraphLoading {
        graph = GraphLoader.edgeListFile(sc, args(0), true, edgeStorageLevel = StorageLevel.MEMORY_AND_DISK,
          vertexStorageLevel = StorageLevel.MEMORY_AND_DISK, minEdgePartitions = minEdgePartitions)
      }

      logPartitioning {
        graph.partitionBy(PartitionStrategy.fromString(partitionerName))
        // just to push partitioner to execute
        graph.collectEdges(EdgeDirection.In)
      }
      logCalculationAfterPartitioning(graph)

      logAlgorithExecution {
      }

      logResultSaving {

      }
    }
  }
}