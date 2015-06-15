package diameter

import org.apache.spark.SparkContext._
import org.apache.spark._
import org.apache.spark.graphx._
import org.apache.spark.storage.StorageLevel

object Tester   {

  def main(args: Array[String]) {

    require(args.length == 2, """|Wrong argument number.
                                 |Should be 1. Usage: <pathToGrpah> <parts>""".stripMargin)

    val pathToGrpah = args(0)
    val minEdgePartitions = args(1).toInt

    val nameOfGraph = pathToGrpah.substring(pathToGrpah.lastIndexOf("/") + 1)

    val sc = new SparkContext(new SparkConf()
      .setSparkHome(System.getenv("SPARK_HOME"))
      .setAppName(s" PartitionAndConnectedCommunity $nameOfGraph cores")
      .setJars(SparkContext.jarOfClass(this.getClass).toList))
    println("Context created")

    var graph: Graph[Int, Int] = null
    var conCom: VertexRDD[VertexId] = null
    graph = GraphLoader.edgeListFile(sc, pathToGrpah, false, edgeStorageLevel = StorageLevel.MEMORY_AND_DISK,
      vertexStorageLevel = StorageLevel.MEMORY_AND_DISK, minEdgePartitions = minEdgePartitions)
    graph.edges.count
    println("Graph loaded")
    val z = graph.vertices.mapValues((id, v) => {
      println("azaza")
      v
    }).collect
    println("z.length = " + z.length)

  }
  
}
