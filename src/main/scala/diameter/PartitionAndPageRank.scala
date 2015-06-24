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

object PartitionAndPageRank {

  def main(args: Array[String]) {

    require(args.length == 5, """|Wrong argument number.
                                 |Should be 5. Usage: <pathToGrpah> <partiotionerName> 
                                 |<filenameWithResult> <minEdgePartitions> <numberOfCores>""".stripMargin)

    val pathToGrpah = args(0)
    val partitionerName = args(1)
    val filenameWithResult = args(2)
    val minEdgePartitions = args(3).toInt
    val numberOfCores = args(4)

    assert(new File(pathToGrpah).isFile(), "it is not a graph: " + pathToGrpah)
    
    val nameOfGraph = pathToGrpah.substring(pathToGrpah.lastIndexOf("/") + 1)

    val sc = new SparkContext(new SparkConf()
      .setSparkHome(System.getenv("SPARK_HOME"))
      .setAppName(s" PartitionAndPageRank $nameOfGraph $partitionerName $numberOfCores cores")
//      .setAppName(" part + conCom  EdgePartition1D 50 cores")
      .set("spark.cores.max", numberOfCores)
      .setJars(SparkContext.jarOfClass(this.getClass).toList))

    var graph: Graph[Int, Int] = null
    var r: Graph[Double,Double] = null
    JsonLogger(sc, filenameWithResult, "") { logger =>
      import logger._

      logGraphLoading {
        graph = GraphLoader.edgeListFile(sc, pathToGrpah, false, edgeStorageLevel = StorageLevel.MEMORY_AND_DISK,
          vertexStorageLevel = StorageLevel.MEMORY_AND_DISK, minEdgePartitions = minEdgePartitions)
      }

      logPartitioning {
        graph = graph.partitionBy(PartitionStrategy.fromString(partitionerName))
        // just to make sure that partition really did
        graph.edges.count
      }
      logCalculationAfterPartitioning(graph)
      logAlgorithExecution {
        r = graph.pageRank(0.1, 0.15)
        r.vertices.count
      }
      logResultSaving{
        r.vertices.coalesce(1, true).saveAsTextFile(args(2) + ".conCom")
      }
      

    }
  }
}
