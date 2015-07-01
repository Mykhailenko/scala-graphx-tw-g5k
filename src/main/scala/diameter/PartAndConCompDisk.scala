package diameter

import org.apache.spark.SparkContext._
import org.apache.spark._
import org.apache.spark.graphx._
import org.apache.spark.storage.StorageLevel
import java.io.PrintWriter
import java.io.PrintWriter
import java.io.FileWriter

object PartAndConCompDisk  {

  def main(args: Array[String]) {

//    require(args.length == 5, """|Wrong argument number.
//                                 |Should be 5. Usage: <pathToGrpah> <partiotionerName> 
//                                 |<filenameWithResult> <minEdgePartitions> <numberOfCores>""".stripMargin)

    val pathToGrpah = args(0)
    val partitionerName = args(1)
    val filenameWithResult = args(2)
    val minEdgePartitions = args(3).toInt
    val numberOfCores = args(4)

    val out = new PrintWriter(new FileWriter(filenameWithResult));
    
    val startTime = System.currentTimeMillis()
    
    val nameOfGraph = pathToGrpah.substring(pathToGrpah.lastIndexOf("/") + 1)

    val sc = new SparkContext(new SparkConf()
      .setSparkHome(System.getenv("SPARK_HOME"))
      .setAppName(s" PartitionAndConnectedCommunity $nameOfGraph $partitionerName $numberOfCores cores")
//      .set("spark.cores.max", numberOfCores)
      .setJars(SparkContext.jarOfClass(this.getClass).toList))
    val t = System.currentTimeMillis()
    out.println("Context created " + (t - startTime) + " ms")
    out.flush()

    var graph: Graph[Int, Int] = null
    var conCom: VertexRDD[VertexId] = null
    graph = GraphLoader.edgeListFile(sc, pathToGrpah, false, edgeStorageLevel = StorageLevel.MEMORY_AND_DISK,
      vertexStorageLevel = StorageLevel.MEMORY_AND_DISK, minEdgePartitions = minEdgePartitions)
    graph.edges.count
    val t0 = System.currentTimeMillis()
    out.println("Graph loaded for " + (t0 - t) + " ms")
    out.flush()
    
    graph = graph.partitionBy(PartitionStrategy.fromString(partitionerName))
    out.println("graph.edges.count = " + graph.edges.count)
    val t1 = System.currentTimeMillis()
    out.println("Graph repartitioned " + (t1 - t0) + " ms")
    out.flush()
    
    conCom = graph.connectedComponents.vertices
    out.println("conCom.count = " + conCom.count)
    val t2 = System.currentTimeMillis()
    out.println("Connected components calculated " + (t2 - t1) + " ms")
    out.flush()
    
    sc.stop

    out.close

  }
  
}
