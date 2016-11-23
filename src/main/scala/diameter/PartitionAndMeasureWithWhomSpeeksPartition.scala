package diameter

import util.JsonLogger0
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

object PartitionAndMeasureWithWhomSpeeksPartition {

  def main(args: Array[String]) {

    require(args.length == 4, """|Wrong argument number.
                                 |Should be 5. Usage: <pathToGrpah> <filenameWithResult> <parititonerName> <minEdgePartitions> <shortGraphName>""".stripMargin)

    val pathToGrpah = args(0)
    val filenameWithResult = args(1)
    val partitionerName = args(2)
    val minEdgePartitions = args(3).toInt
    
    //val parts = args(5)

    val sc = new SparkContext(new SparkConf()
      .setSparkHome(System.getenv("SPARK_HOME"))
      .setJars(SparkContext.jarOfClass(this.getClass).toList))

    var graph: Graph[Int, Int] = null

    graph = GraphLoader.edgeListFile(sc, pathToGrpah, false, edgeStorageLevel = StorageLevel.MEMORY_AND_DISK,
      vertexStorageLevel = StorageLevel.MEMORY_AND_DISK, minEdgePartitions = minEdgePartitions)

    if(partitionerName != "no"){
        graph = graph.partitionBy(PartitionStrategy.fromString(partitionerName))
    }
    graph.numEdges

    val out = new PrintWriter(new FileWriter(filenameWithResult, true));
    
    
    val pairs = graph.edges.mapPartitionsWithIndex((x : Int, y : Iterator[Edge[Int]]) => {
      y.flatMap( edge => Array((edge.srcId, x), (edge.dstId, x)))
    }, true).coalesce(1, true).groupByKey().map(x => (x._1, x._2.toSet))
    
    val ids = pairs.values.flatMap { x => x }.distinct()
    require(ids.count() == minEdgePartitions, "But they have to be equal")
    
    val answer = ids.collect.map(partid => {
//      out.println("partid " + partid)
      val pre = pairs.filter(x => x._2.contains(partid))
           .flatMap(x => x._2)
           .distinct()
           .count
           
//      pre.map(x => {
//        out.println(x)
//      })
      pre
    })
    answer.foreach(x => {
      out.println(x)
    })

    out.close

  }
}
