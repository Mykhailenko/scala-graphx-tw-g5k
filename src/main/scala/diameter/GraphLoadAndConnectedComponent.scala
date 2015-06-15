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

object GraphLoadAndConnectedComponent {

  def main(args: Array[String]) {

    val startAll = System.currentTimeMillis()
    require(args.length == 5, """|Wrong argument number.
                                 |Should be 5. Usage: <pathToGrpah> <partiotionerName> 
                                 |<filenameWithResult> <minEdgePartitions> <numberOfCores>""".stripMargin)

    val pathToGrpah = args(0)
    val partitionerName = args(1)
    val filenameWithResult = args(2)
    val minEdgePartitions = args(3).toInt
    val numberOfCores = args(4)

    val nameOfGraph = pathToGrpah.substring(pathToGrpah.lastIndexOf("/") + 1)

    val sc = new SparkContext(new SparkConf()
      .setSparkHome(System.getenv("SPARK_HOME"))
//      .setAppName(s" part + conCom | $nameOfGraph | $partitionerName | $minEdgePartitions parts | $numberOfCores cores")
      .set("spark.cores.max", numberOfCores)
      .setJars(SparkContext.jarOfClass(this.getClass).toList))

    var graph: Graph[Int, Int] = null
    var conCom: VertexRDD[VertexId] = null

    val startLoad = System.currentTimeMillis()
    graph = GraphLoader.edgeListFile(sc, pathToGrpah, true, edgeStorageLevel = StorageLevel.MEMORY_AND_DISK,
      vertexStorageLevel = StorageLevel.MEMORY_AND_DISK, minEdgePartitions = minEdgePartitions)
    graph.edges.count
    val endLoad = System.currentTimeMillis()
    graph = graph.partitionBy(PartitionStrategy.fromString(partitionerName))
    graph.edges.count
    val endPart = System.currentTimeMillis()
    
    // just to make sure that partition really did
    conCom = graph.connectedComponents.vertices
    conCom.count
    val endCom = System.currentTimeMillis()
    
    conCom.coalesce(1, true).saveAsTextFile(filenameWithResult + ".concom")

    val endSaving = System.currentTimeMillis()
    
    val timeAll = endSaving - startAll
    val timeLoad = endLoad - startLoad
    val timePartition = endPart - endLoad
    val timeCom = endCom - endPart
    val timeSave = endSaving - endCom
    println("\n\n\n")
    println(s"all: $timeAll; load: $timeLoad; partition: $timePartition; com: $timeCom; save: $timeSave.")  
    println("\n\n\n")
  }
}