package util

import org.apache.spark.SparkContext._
import org.apache.spark._
import org.apache.spark.graphx._
import org.apache.spark.rdd.RDD
import scala.collection.immutable.Vector
import sun.security.provider.certpath.Vertex
import java.io.File
import java.io.PrintWriter
import java.io.FileWriter
import org.apache.spark.graphx.impl.EdgePartition
import java.util.Random


case class JsonLogger(sparkContex: SparkContext, path: String = "./")(body: JsonLogger => Unit) extends Logger {

  var mainObj: Map[String, String] = Map()

  def log(key: String, value: String) {
    mainObj += (key -> value)
  }

  def presave {
    mainObj += ("master" -> sparkContex.master)
    mainObj += ("name" -> sparkContex.appName)

  }

  def fileName: String = "0" + (new Random().nextLong() % 1000).toString + ".json"

  def save {
    val out = new PrintWriter(new FileWriter(new File(path + fileName)));
    out println "{"
    val lst: List[(String, String)] = mainObj.toList
    for (i <- 0 until lst.length - 1; (key, value) = lst(i)) {
      out println "\t\"" + key + "\" : \"" + value + "\","
    }
    val (key, value) = lst(lst.length - 1)
    out println "\t\"" + key + "\" : \"" + value + "\""
    out println "}"
    out close
  }

  def calculateReplicatoins(graph: Graph[Int, Int]) = {
    val q = graph.edges.partitionsRDD
    val p = q.mapValues((V) => (Set(V.srcIds: _*) ++ Set(V.dstIds: _*)).size)
    val w = p.map(a => a._2)
    val replic: Double = w.reduce((a, b) => a + b)
    replic 
  }
  def numberOfVerices(graph: Graph[Int, Int]): Long = {
    graph.numVertices
  }
  def numberOfEdges(graph: Graph[Int, Int]): Long = {
    graph.numEdges
  }

  def partitioningNumber(graph: Graph[Int, Int]) = {
    graph.edges.partitionsRDD.count
  }
  
  def calculateBalance(graph: Graph[Int, Int]) : Double = {
    val maxPart : Double = (graph.edges.partitionsRDD.mapValues(V => V.dstIds.length).map(a => a._2).max())
    val averagePart : Double = (numberOfEdges(graph) / partitioningNumber(graph))
    maxPart / averagePart
  }

  def logCalculationAfterPartitioning(graph: Graph[Int, Int]) = {
    log("replicationFactor", (calculateReplicatoins(graph) / numberOfVerices(graph)).toString)
    log("balance", calculateBalance(graph).toString)
    log("numberVertices", numberOfVerices(graph).toString)
    log("numberEdges", numberOfEdges(graph).toString)
    log("numberPartitions", partitioningNumber(graph).toString)
  }

  override def logPartitioning(body: => Unit) = {
    logtime("Partitioning")(body)
  }

  body(this)
  presave
  save

}
