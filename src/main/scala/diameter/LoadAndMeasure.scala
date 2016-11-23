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

object LoadAndMeasure {

  def main(args: Array[String]) {

    require(args.length == 2, """|Wrong argument number.
                                 |Should be 2. Usage: <pathToGrpah> <filenameWithResult>""".stripMargin)

    val pathToGraph = args(0)
    val filenameWithResult = args(1)

    val sc = new SparkContext(new SparkConf()
      .setSparkHome(System.getenv("SPARK_HOME"))
      .setJars(SparkContext.jarOfClass(this.getClass).toList))

    val edges : RDD[Edge[Any]] = sc.objectFile[Edge[Any]](pathToGraph)

    val graph = Graph.fromEdges(edges, 0).mapVertices((vid, value) => 1)
      .mapEdges((pid, edges) => edges.map(edge => 1))

      
      
    val jaylo = new JsonLogger0(sc)
    val mapa = jaylo.metrics(graph)

    val out = new PrintWriter(new FileWriter(filenameWithResult, true));
    val arr = Array("numberPartitions", "balance", "replicationFactor",
      "NSTDEV", "largestPartition", "vertexNonCutted", "vertexCut", "communicationCost")
    val vals = pathToGraph :: graph.numVertices.toString :: graph.numEdges.toString :: arr.map(x => mapa.get(x).getOrElse("")).toList
    val s = vals.mkString(";")
    out.println(s)
    out.close
  }
}
