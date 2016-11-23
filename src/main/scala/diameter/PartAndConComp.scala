package diameter

import org.apache.spark.SparkContext._
import org.apache.spark._
import org.apache.spark.graphx._
import org.apache.spark.storage.StorageLevel
import java.io.PrintWriter
import java.io.PrintWriter
import java.io.FileWriter

object PartAndConComp {
  def main(args: Array[String]) {
    val pathToGrpah = args(0)
    val partitionerName = args(1)
    val filenameWithResult = args(2)
    val minEdgePartitions = args(3).toInt
    
    val out = new PrintWriter(new FileWriter(filenameWithResult));
    val startTime = System.currentTimeMillis()
    val nameOfGraph = pathToGrpah.substring(pathToGrpah.lastIndexOf("/") + 1)

    val sc = new SparkContext(new SparkConf()
      .setSparkHome(System.getenv("SPARK_HOME"))
      .setJars(SparkContext.jarOfClass(this.getClass).toList))
    
    val t = System.currentTimeMillis()
    var graph: Graph[Int, Int] = null
    var conCom: VertexRDD[VertexId] = null
    graph = GraphLoader.edgeListFile(sc, pathToGrpah, false,
      edgeStorageLevel = StorageLevel.MEMORY_AND_DISK,
      vertexStorageLevel = StorageLevel.MEMORY_AND_DISK)
//      numEdgePartitions = minEdgePartitions) //8 //0
      graph.numVertices
    graph.edges.count // 1
    
    val t0 = System.currentTimeMillis()
    out.println("Graph loaded for " + (t0 - t) + " ms")
    out.flush()
    graph = graph.partitionBy(PartitionStrategy.fromString(partitionerName))//.persist() // 9 // 2 // 10
    out.println("graph.edges.c ount = " + graph.edges.count) // 3
    val t1 = System.currentTimeMillis()
    out.println("Graph repartitioned " + (t1 - t0) + " ms")
    out.flush()
    conCom = graph.connectedComponents.vertices
    out.println("conCom.count = " + conCom.count)
    val t2 = System.currentTimeMillis()
    out.println("Connected components calculated " + (t2 - t1) + " ms")
    out.flush()
    conCom.map(x => x._1 + " " + x._2).coalesce(1).saveAsTextFile(filenameWithResult + "actual")
    val differentCommunities = conCom.groupBy(t => t._2).map(x => x._2.size).count
    out.println("differentCommunities = " + differentCommunities)
    out.flush()
    sc.stop
    out.close
  }
}
