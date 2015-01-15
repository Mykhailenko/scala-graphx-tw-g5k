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
import com.google.common.collect.Sets
import scala.collection.JavaConversions

case class JsonLogger(sparkContex: SparkContext, fileName: String = "0" + (new Random().nextLong() % 1000).toString + ".json", path: String = "./")(body: JsonLogger => Unit) extends Logger {

  var mainObj: Map[String, String] = Map()

  def log(key: String, value: String) {
    mainObj += (key -> value)
  }

  def presave {
    mainObj += ("master" -> sparkContex.master)
    mainObj += ("name" -> sparkContex.appName)

  }

  //  def fileName: String = "0" + (new Random().nextLong() % 1000).toString + ".json"

  def save {
    println("path: " + path)
    println("fileName: " + fileName)

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

  def calculateSetOfVerticesNotCatted(graph: Graph[Int, Int]): Set[VertexId] = {
    val q = graph.edges.partitionsRDD
    val p = q.mapValues((V) => (Set(V.srcIds: _*) ++ Set(V.dstIds: _*)))
    val w = p.map(a => a._2)
    val n = w.reduce(
      (a, b) =>
        a.union(b).diff(a.intersect(b))
        )
    n
  }

  def calculateSetOfVertices(graph: Graph[Int, Int]): Set[VertexId]= {
    val ar : Array[VertexId] = graph.vertices.collect.map( id => id._1)
    Set(ar : _*)
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

  def partitioningNumber(graph: Graph[Int, Int]) : Long = {
    graph.edges.partitionsRDD.count
  }

  def maxPartitionSize(graph: Graph[Int, Int]): Long = {
    graph.edges.partitionsRDD.mapValues(V => V.dstIds.length).map(a => a._2).max()
  }

  def calculateBalance(graph: Graph[Int, Int]): Double = {
    val maxPart: Long = maxPartitionSize(graph)
    val averagePart: Long = (numberOfEdges(graph) / partitioningNumber(graph))
    maxPart / averagePart
  }
  def numberOfEdgesInEachPartition(graph: Graph[Int, Int]): Array[Int] = {

    graph.edges.partitionsRDD.mapValues(V => V.dstIds.length).map(a => a._2).collect

  }

  
  def calculateSetOfVerticesCatted(graph: Graph[Int, Int]): Set[VertexId] = {
    calculateSetOfVertices(graph).diff(calculateSetOfVerticesNotCatted(graph))
  }
  
  def communicationalCost(graph: Graph[Int, Int]): Long = {
    val temp = calculateSetOfVerticesNotCatted(graph)
    // Sum of Fi over i
    val q = graph.edges.partitionsRDD
    val p = q.mapValues((V) => (Set(V.srcIds: _*) ++ Set(V.dstIds: _*)))
    val w = p.map(a => a._2)
    val z = w.map(a => a.diff(temp))
    val s = z.map(a => a.size)
    s.reduce(_ + _)
  }
  
  def NSTDEV(graph: Graph[Int, Int]): Double = {
    Math.sqrt(numberOfEdgesInEachPartition(graph).toList.map(e => Math.pow(e * partitioningNumber(graph) / numberOfEdges(graph) - 1, 2)).sum / partitioningNumber(graph))
  }
  def logCalculationAfterPartitioning(graph: Graph[Int, Int]) = {
    log("replicationFactor", (calculateReplicatoins(graph) / numberOfVerices(graph)).toString)
    log("balance", calculateBalance(graph).toString)
    log("communicationCost", communicationalCost(graph).toString)
    log("vertexCut", (calculateSetOfVerticesCatted(graph).size).toString)
    log("vertexNonCutted", (calculateSetOfVerticesNotCatted(graph).size).toString)
    log("numberVertices", numberOfVerices(graph).toString)
    log("maxPartition", maxPartitionSize(graph).toString)
    log("numberEdges", numberOfEdges(graph).toString)
    log("numberPartitions", partitioningNumber(graph).toString)
    log("edgeInPartitiones", numberOfEdgesInEachPartition(graph).toList.mkString(", "))
    log("NSTDEV", NSTDEV(graph).toString)
  }

  override def logPartitioning(body: => Unit) = {
    logtime("Partitioning")(body)
  }

  body(this)
  presave
  save

}
