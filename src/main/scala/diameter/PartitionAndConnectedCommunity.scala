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

object PartitionAndConnectedCommunity {

  def main(args: Array[String]) {

    require(args.length == 5, """|Wrong argument number.
                                 |Should be 4. Usage: <pathToGrpah> <partiotionerName> 
                                 |<filenameWithResult> <minEdgePartitions> <numberOfCores>""".stripMargin)

    val pathToGrpah = args(0)
    val partitionerName = args(1)
    val filenameWithResult = args(2)
    val minEdgePartitions = args(3).toInt
    val numberOfCores = args(4)

    val nameOfGraph = pathToGrpah.substring(pathToGrpah.lastIndexOf("/") + 1)
    
    val sc = new SparkContext(new SparkConf()
      .setSparkHome(System.getenv("SPARK_HOME"))
      .setAppName(s" part + conCom | $nameOfGraph | $partitionerName | $minEdgePartitions parts | $numberOfCores cores")
      .set("spark.cores.max", numberOfCores)
      .setJars(SparkContext.jarOfClass(this.getClass).toList))

    var graph: Graph[Int, Int] = null
    var conCom: Array[(VertexId, VertexId)] = null

    JsonLogger(sc, filenameWithResult, "") { logger =>
      import logger._

      logGraphLoading {
        graph = GraphLoader.edgeListFile(sc, pathToGrpah, true, edgeStorageLevel = StorageLevel.MEMORY_AND_DISK,
          vertexStorageLevel = StorageLevel.MEMORY_AND_DISK, minEdgePartitions = minEdgePartitions)
      }

      logPartitioning {
        graph = graph.partitionBy(PartitionStrategy.fromString(partitionerName))
        // just to make sure that partition really did
        graph.edges.collect
        graph.edges.partitionsRDD.collect
      }
      logCalculationAfterPartitioning(graph)
      logAlgorithExecution {
        conCom = graph.connectedComponents.vertices.collect
      }
      logResultSaving {
        val out = new PrintWriter(new FileWriter(new File(filenameWithResult + ".concom")));

        for ((id, componentId) <- conCom) {
          out println s"$id $componentId"
        }
        out close

      }

    }
  }
}