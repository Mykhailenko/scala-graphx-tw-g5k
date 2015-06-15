package diameter

import org.apache.spark.SparkContext._
import org.apache.spark._
import org.apache.spark.graphx._
import org.apache.spark.storage.StorageLevel

object FuckPartitionAndConnectedComponent {

  def main(args: Array[String]) {

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
      .setAppName(s" PartitionAndConnectedCommunity $nameOfGraph $partitionerName $numberOfCores cores")
      .set("spark.cores.max", numberOfCores)
      .setJars(SparkContext.jarOfClass(this.getClass).toList))
    println("Context created")

    var graph: Graph[Int, Int] = null
    var conCom: VertexRDD[VertexId] = null
    graph = GraphLoader.edgeListFile(sc, pathToGrpah, false, edgeStorageLevel = StorageLevel.MEMORY_ONLY,
      vertexStorageLevel = StorageLevel.MEMORY_ONLY, minEdgePartitions = minEdgePartitions)
    graph.edges.count
    println("Graph loaded")
    
    graph = graph.partitionBy(PartitionStrategy.fromString(partitionerName))
    graph.edges.count
    println("Graph repartitioned")
    
    conCom = graph.connectedComponents.vertices
    println("Connected components calculated")

    println("conCom.count = " + conCom.count)
    
    conCom.coalesce(1, true).saveAsTextFile(args(2) + ".conCom")

  }
  
}
