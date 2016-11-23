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

object PartitionAndMeasureAndSave {

  def main(args: Array[String]) {

    require(args.length == 5, """|Wrong argument number.
                                 |Should be 5. Usage: <pathToGrpah> <filenameWithResult> <parititonerName> <minEdgePartitions> <saveDirectory>""".stripMargin)

    val pathToGraph = args(0)
    val filenameWithResult = args(1)
    val partitionerName = args(2)
    val minEdgePartitions = args(3).toInt
    val saveDirectory = args(4)

    val sc = new SparkContext(new SparkConf()
      .setSparkHome(System.getenv("SPARK_HOME"))
      .setJars(SparkContext.jarOfClass(this.getClass).toList))

    var graph: Graph[Int, Int] = null

    graph = GraphLoader.edgeListFile(sc, pathToGraph, false, edgeStorageLevel = StorageLevel.MEMORY_AND_DISK,
      vertexStorageLevel = StorageLevel.MEMORY_AND_DISK, minEdgePartitions = minEdgePartitions)

    if(partitionerName != "no"){
        graph = graph.partitionBy(PartitionStrategy.fromString(partitionerName))
    }
    graph.numEdges

    val jaylo = new JsonLogger0(sc)
    val mapa = jaylo.metrics(graph)

    val out = new PrintWriter(new FileWriter(filenameWithResult, true));
    val arr = Array("numberPartitions", "balance", "replicationFactor",
      "NSTDEV", "largestPartition", "vertexNonCutted", "vertexCut", "communicationCost")
    val vals = pathToGraph :: partitionerName :: graph.numVertices.toString :: graph.numEdges.toString :: arr.map(x => mapa.get(x).getOrElse("")).toList
    val s = vals.mkString(";")
    out.println(s)
    out.close

    graph.edges.saveAsObjectFile(saveDirectory)
  }
}
