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

  def saveCSV(graph: Graph[Int, Int]) {
    val out = new PrintWriter(new FileWriter(new File(path + fileName + ".csv")));

    out.println("Vertex degree distribution , ")
    out.println("Vertex degree, number of vertices")

    var data = calculcateAverageDegree(graph)

    for (line <- data) {
      out.println(line._1 + " , " + line._2)
    }

    out close
  }

  def calculcateAverageDegree(graph: Graph[Int, Int]): Array[(Int, Int)] = {
    graph.degrees.collect.groupBy(_._2).map(a => (a._1, a._2.length)).toArray.sortWith(_._1 < _._1)
  }

  def calculateSetOfVerticesNotCatted(graph: Graph[Int, Int]): Set[VertexId] = {
    val q = graph.edges.partitionsRDD
    val p = q.mapValues((V) => (Set(V.srcIds: _*) ++ Set(V.dstIds: _*)))
    val w = p.map(a => a._2)
    //    val n = w.reduce(
    //      (a, b) =>
    //        a.union(b).diff(a.intersect(b))
    //        )
    var ss = w.collect
    var qq = ss.foldLeft(List[VertexId]())((a, b) => a.toList ++ b.toList)
    var tr = qq.groupBy(identity).mapValues(v => v.length)
    var result = tr.filter(a => a._2 == 1).map(x => x._1).toSet

    val out = new PrintWriter(new FileWriter(new File("./debugmfk")));
    out.println("set of non cutted vertices")
    for (id <- result) {
      out.println(id)
    }
    out.flush
    out.close
    result
  }

  def calculateSetOfVertices(graph: Graph[Int, Int]): Set[VertexId] = {
    val ar: Array[VertexId] = graph.vertices.collect.map(id => id._1)
    Set(ar: _*)
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

  def partitioningNumber(graph: Graph[Int, Int]): Long = {
    graph.edges.partitionsRDD.count
  }

  def maxPartitionSize(graph: Graph[Int, Int]): Long = {
    graph.edges.partitionsRDD.mapValues(V => V.dstIds.length).map(a => a._2).max()
  }

  def calculateBalance(graph: Graph[Int, Int]): Double = {
    val maxPart: Double = maxPartitionSize(graph)
    val averagePart: Double = (numberOfEdges(graph) / partitioningNumber(graph))
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
    var ar : List[Double] = numberOfEdgesInEachPartition(graph).toList.map(a => a.toDouble)
    Math.sqrt(ar.map(e => Math.pow(e * partitioningNumber(graph) / numberOfEdges(graph) - 1, 2)).sum / partitioningNumber(graph))
  }
  def logCalculationAfterPartitioning(graph: Graph[Int, Int]) = {
    log("replicationFactor", (calculateReplicatoins(graph) / numberOfVerices(graph)).toString)
    log("balance", calculateBalance(graph).toString)
    log("communicationCost", communicationalCost(graph).toString)
    log("vertexCut", (calculateSetOfVerticesCatted(graph).size).toString)
    log("vertexNonCutted", (calculateSetOfVerticesNotCatted(graph).size).toString)
    log("numberVertices", numberOfVerices(graph).toString)
    log("largestPartition", maxPartitionSize(graph).toString)
    log("numberEdges", numberOfEdges(graph).toString)
    log("numberPartitions", partitioningNumber(graph).toString)
    log("edgeInPartitiones", numberOfEdgesInEachPartition(graph).toList.mkString(", "))
    log("NSTDEV", NSTDEV(graph).toString)
//    saveCSV(graph)
  }

  override def logPartitioning(body: => Unit) = {
    logtime("Partitioning")(body)
  }

  body(this)
  presave
  save

}
