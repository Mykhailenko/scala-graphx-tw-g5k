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

    require(args.length >= 4, "Wrong argument number. Should be 4. Usage: <path_to_grpah> <partiotioner_name> <filename_with_result> <minEdgePartitions> [true/false for save partitions]")

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
        graph = graph.partitionBy(PartitionStrategy.fromString(partitionerName))
        if (args.length == 5 && args(4) == "true") {
          var xxx = graph.edges.partitionsRDD.mapValues(b => (b.srcIds, b.dstIds).zipped map ((_, _)))
          val out = new PrintWriter(new FileWriter(new File(args(2) + ".partition")));
          out.println("There are " + xxx.count + " partitions");
          for ((id, edges) <- xxx.collect) {
            out.println("Partition id " + id.toLong)
            for ((src, dst) <- edges) {
              out.println(src + " --> " + dst)
            }
          }
          out.flush();
          out.close()
        }
      }
      logCalculationAfterPartitioning(graph)

      logAlgorithExecution {
      }
      val inDegrees: VertexRDD[Int] = graph.inDegrees

      logResultSaving {

      }
    }
  }
}