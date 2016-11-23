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

case class JsonLogger(sparkContex: SparkContext,
  fileName: String = "0" + (new Random().nextLong() % 1000).toString + ".json",
  path: String = "./")(body: JsonLogger => Unit) extends Logger {

  val appStartTime = System.currentTimeMillis();

  var mainObj: Map[String, String] = Map()


  def log(key: String, value: String) {
    mainObj += (key -> value)
    save
  }

  def presave {
    val appEndTime = System.currentTimeMillis()
    mainObj += ("master" -> sparkContex.master)
    mainObj += ("name" -> sparkContex.appName)
    mainObj += ("applicationTime.start" -> appStartTime.toString)
    mainObj += ("applicationTime.end" -> appEndTime.toString)
    mainObj += ("applicationTime.time" -> (appEndTime - appStartTime).toString)

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
    val result = graph.degrees.collect.groupBy(_._2).map(a => (a._1, a._2.length)).toArray.sortWith(_._1 < _._1)
//    assert(result.map(x => x._2).reduce(_ + _) == numberOfVerices(graph))
    result
  }

  def numberOfVerticesCanBeCut(graph: Graph[Int, Int]): Long = {
    val degree2count = calculcateAverageDegree(graph)
    degree2count.filter(x => x._1 > 1).map(x => x._2).reduce(_ + _)
  }

  def calculateSetOfVerticesNotCatted(graph: Graph[Int, Int]): Set[VertexId] = {
    val q = graph.edges.partitionsRDD
    val p = q.mapValues((V) => (Set(V.srcIds: _*) ++ Set(V.dstIds: _*)))
    val w = p.map(a => a._2).flatMap(a => a)
    //    val n = w.reduce(
    //      (a, b) =>
    //        a.union(b).diff(a.intersect(b))
    //        )

    //log("w size", w.count.toString)
    var result = w.countByValue.filter(_._2 == 1).map(x => x._1).toSet//.reduce((a, b) => a ++ b)
    //var ss = w.collect /// fails here. collection is too big
    //var qq = ss.foldLeft(List[VertexId]())((a, b) => a.toList ++ b.toList)
    //var tr = qq.groupBy(identity).mapValues(v => v.length)
    //var result = tr.filter(a => a._2 == 1).map(x => x._1).toSet
    result
  }

  def calculateSetOfVertices(graph: Graph[Int, Int]): Set[VertexId] = {
    val ar: Array[VertexId] = graph.vertices.collect.map(id => id._1)
    Set(ar: _*)
  }

  def calculateReplicatoins(graph: Graph[Int, Int]) = {
    val ge = graph.edges
    val q = ge.partitionsRDD
    val p = q.mapValues((V) => (Set(V.srcIds: _*) ++ Set(V.dstIds: _*)).size)
    val w = p.map(a => a._2.toLong)
    val replic: Long = w.reduce((a, b) => a + b)
    log("calculateReplicatoins", replic.toString)
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

  def communicationalCost(graph: Graph[Int, Int], notCatted : Set[VertexId]): Long = {
    // Sum of Fi over i
    val q = graph.edges.partitionsRDD
    val p = q.mapValues((V) => (Set(V.srcIds: _*) ++ Set(V.dstIds: _*)))
    val w = p.map(a => a._2)
    val z = w.map(a => a.diff(notCatted))
    val s = z.map(a => a.size.toLong)
    s.reduce(_ + _)
  }

  def NSTDEV(graph: Graph[Int, Int]): Double = {
    var ar : List[Double] = numberOfEdgesInEachPartition(graph).toList.map(a => a.toDouble)
    Math.sqrt(ar.map(e => Math.pow(e * partitioningNumber(graph) / numberOfEdges(graph) - 1, 2)).sum / partitioningNumber(graph))
  }
  def logCalculationAfterPartitioning(graph: Graph[Int, Int]) = 0
  
  def metrics(graph: Graph[Int, Int]) : Map[String, String] = {
    val nov = numberOfVerices(graph)
    log("numberVertices", nov.toString)
    log("numberEdges", numberOfEdges(graph).toString)
    log("numberPartitions", partitioningNumber(graph).toString)

    log("balance", calculateBalance(graph).toString)
    log("replicationFactor", (calculateReplicatoins(graph) / nov).toString)
    log("NSTDEV", NSTDEV(graph).toString)
    log("largestPartition", maxPartitionSize(graph).toString)
    val notCatted = calculateSetOfVerticesNotCatted(graph)
    log("vertexNonCutted", notCatted.size.toString)

    log("vertexCut", (nov - notCatted.size).toString)
    log("communicationCost", communicationalCost(graph, notCatted).toString)

    log("edgeInPartitiones", numberOfEdgesInEachPartition(graph).toList.mkString(", "))
    mainObj
    //log("numberVerticesCanBeCut", numberOfVerticesCanBeCut(graph).toString)
//    saveCSV(graph)
  }

  override def logPartitioning(body: => Unit) = {
    logtime("Partitioning")(body)
  }

  body(this)
  presave
  save

}
