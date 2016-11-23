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

object PartitionAndMeasure {

  def main(args: Array[String]) {

    require(args.length == 5, """|Wrong argument number.
                                 |Should be 5. Usage: <pathToGrpah> <filenameWithResult> <parititonerName> <minEdgePartitions> <shortGraphName>""".stripMargin)

    val pathToGrpah = args(0)
    val filenameWithResult = args(1)
    val partitionerName = args(2)
    val minEdgePartitions = args(3).toInt
    val graphName = args(4)
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

    val jaylo = new JsonLogger0(sc)
    val mapa = jaylo.metrics(graph)

    //val partss = jaylo.numberOfEdgesInEachPartition(graph).mkString(";")
    //val op = new PrintWriter(new FileWriter(parts, false));
    //op.println(partss)
    //op.close

    val out = new PrintWriter(new FileWriter(filenameWithResult, true));
    val arr = Array(
        "numberVertices",
        "numberEdges",
        "numberPartitions",
        "balance",
        "NEWlargestPartition",
//        "replicationFactor",
        "NSTDEV",
//      "largestPartition", "vertexNonCutted",
        "vertexCut",
        "communicationCost"
//      "average-degree-not-cut",
//      "normalized-cut-dst-or-src",
//      "normalized-cut-dst",
//      "normalized-cut-src",
//      "normalized-cut-dst-and-src",
//      "normalized-cut-dst-not-src",
//      "normalized-cut-src-not-dst",
//      "amount-of-remote-vertices"

      )




    val vals = graphName :: partitionerName :: arr.map(x => mapa.get(x).getOrElse("")).toList //:: graph.numVertices.toString :: graph.numEdges.toString
    val s = vals.mkString(";")

    out.println(s)
    out.close

  }
}
