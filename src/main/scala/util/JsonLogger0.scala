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

class JsonLogger0(sparkContex: SparkContext) {

  var mainObj: Map[String, String] = Map()

  def log(key: String, value: String) {
    mainObj += (key -> value)
  }

  def calculcateAverageDegree(graph: Graph[Int, Int]): Array[(Int, Int)] = {
    val result = graph.degrees.collect.groupBy(_._2).map(a => (a._1, a._2.length)).toArray.sortWith(_._1 < _._1)
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
    var result = w.countByValue.filter(_._2 == 1).map(x => x._1).toSet //.reduce((a, b) => a ++ b)
    result
  }

  def degrees(graph: Graph[Int, Int], vertices: Set[VertexId]): List[(Int, Int)] = {
    val bv = sparkContex.broadcast(vertices)
    val verticesWereNotCut = graph.degrees.filter(v => {
      bv.value.contains(v._1)
    })
    val g = verticesWereNotCut.collect.groupBy(_._2).map(a => (a._1, a._2.length)).toArray
    g.sortWith(_._1 < _._1).toList
  }

  def edgesCutDstOrSrc(graph: Graph[Int, Int], vertices: Set[VertexId]): Long = {
    val bv = sparkContex.broadcast(vertices)
    val edges = graph.edges.filter(edge => !bv.value.contains(edge.srcId) || !bv.value.contains(edge.dstId))
    edges.count
  }

  def edgesCutDst(graph: Graph[Int, Int], vertices: Set[VertexId]): Long = {
    val bv = sparkContex.broadcast(vertices)
    val edges = graph.edges.filter(edge => !bv.value.contains(edge.dstId))
    edges.count
  }

  def edgesCutSrc(graph: Graph[Int, Int], vertices: Set[VertexId]): Long = {
    val bv = sparkContex.broadcast(vertices)
    val edges = graph.edges.filter(edge => !bv.value.contains(edge.srcId))
    edges.count
  }

  def edgesCutDstAndSrc(graph: Graph[Int, Int], vertices: Set[VertexId]): Long = {
    val bv = sparkContex.broadcast(vertices)
    val edges = graph.edges.filter(edge => !bv.value.contains(edge.srcId) && !bv.value.contains(edge.dstId))
    edges.count
  }

  def edgesCutDstNotSrc(graph: Graph[Int, Int], vertices: Set[VertexId]): Long = {
    val bv = sparkContex.broadcast(vertices)
    val edges = graph.edges.filter(edge => bv.value.contains(edge.srcId) && !bv.value.contains(edge.dstId))
    edges.count
  }

  def edgesCutSrcNotDst(graph: Graph[Int, Int], vertices: Set[VertexId]): Long = {
    val bv = sparkContex.broadcast(vertices)
    val edges = graph.edges.filter(edge => !bv.value.contains(edge.srcId) && bv.value.contains(edge.dstId))
    edges.count
  }

  def calculateSetOfVertices(graph: Graph[Int, Int]): Set[VertexId] = {
    val ar: Array[VertexId] = graph.vertices.collect.map(id => id._1)
    Set(ar: _*)
  }

  def calculateAveragePartition(graph: Graph[Int, Int]) = {
    val ge = graph.edges
    val pairs = ge.mapPartitionsWithIndex((x : Int, y : Iterator[Edge[Int]]) => {
      y.flatMap( edge => Array((edge.srcId, x), (edge.dstId, x)))
    }, true).groupByKey().map(x => (x._1, x._2.toSet))
    val ids = pairs.values.flatMap { x => x }.distinct()
    require(ids.count() == partitioningNumber(graph), "azaza")
    
    val answer = ids.collect.map(partid => {
      pairs.filter(x => x._2.contains(partid))
           .flatMap(x => x._2).distinct().count
    })
    
    
  }
  
  def calculateReplicatoins(graph: Graph[Int, Int]) = {
    val ge = graph.edges
    val q = ge.partitionsRDD
    val p = q.mapValues((V) => (Set(V.srcIds: _*) ++ Set(V.dstIds: _*)).size)
    val w = p.map(a => a._2.toLong)
    val replic: Long = w.reduce((a, b) => a + b)
    //    log("calculateReplicatoins", replic.toString)
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

  def maxNumberOfVertices(graph: Graph[Int, Int]): Double = {

    val nuniqueVerticesInEdgePartitions = graph.edges
      .partitionsRDD
      .mapValues(V => (Set(V.srcIds: _*) ++ Set(V.dstIds: _*)).size)
      .map(x => x._2.toLong)

    val maxNUniqueVerticesInEdgePartitions = nuniqueVerticesInEdgePartitions.max

    val sumNUniqueVerticesInEdgePartitions = nuniqueVerticesInEdgePartitions.reduce(_ + _)

    val averageNUniqueVerticesInEdgePartitions = sumNUniqueVerticesInEdgePartitions / partitioningNumber(graph)

    val balance = maxNUniqueVerticesInEdgePartitions.toDouble / averageNUniqueVerticesInEdgePartitions.toDouble

    balance
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

  def communicationalCost(graph: Graph[Int, Int], notCatted: Set[VertexId]): Long = {
    // Sum of Fi over i
    val q = graph.edges.partitionsRDD
    val p = q.mapValues((V) => (Set(V.srcIds: _*) ++ Set(V.dstIds: _*)))
    val w = p.map(a => a._2)
    val z = w.map(a => a.diff(notCatted))
    val s = z.map(a => a.size.toLong)
    s.reduce(_ + _)
  }

  def remoteVerteices(graph: Graph[Int, Int]): Long = {

    val idAndESet = graph.edges.partitionsRDD.mapValues((V) => (Set(V.srcIds: _*) ++ Set(V.dstIds: _*))).map(_._2)
    val idAndVSet = graph.vertices.partitionsRDD.map(x => x.iterator.toList.map(x => x._1).toSet)
    val zipped = idAndESet.zipPartitions(idAndVSet, true) ((x: Iterator[Set[VertexId]], y :Iterator[Set[VertexId]]) => {
      x.hasNext
      y.hasNext
      val xSet = x.next()
      val ySet = y.next()
      val local = xSet.intersect(ySet).size
      val all = xSet.size
      val remote = all - local
      Iterator(remote)
    })
    val r = zipped.reduce(_ + _)
    r
  }

  def NSTDEV(graph: Graph[Int, Int]): Double = {
    var ar: List[Double] = numberOfEdgesInEachPartition(graph).toList.map(a => a.toDouble)
    Math.sqrt(ar.map(e => Math.pow(e * partitioningNumber(graph) / numberOfEdges(graph) - 1, 2)).sum /
      partitioningNumber(graph))
  }
  def metrics(graph: Graph[Int, Int]): Map[String, String] = {
    val nov = numberOfVerices(graph)
    log("numberVertices", nov.toString)
    log("numberEdges", numberOfEdges(graph).toString)
    log("numberPartitions", partitioningNumber(graph).toString)
//
    log("balance", calculateBalance(graph).toString)
//    log("replicationFactor", (calculateReplicatoins(graph).toDouble / nov).toString)
    log("NEWlargestPartition", maxNumberOfVertices(graph).toString)
    log("NSTDEV", NSTDEV(graph).toString)
//    log("largestPartition", maxPartitionSize(graph).toString)
    val notCatted = calculateSetOfVerticesNotCatted(graph)
//    log("vertexNonCutted", notCatted.size.toString)
    log("vertexCut", (nov - notCatted.size).toString)
    log("communicationCost", communicationalCost(graph, notCatted).toString)
//
//    val ds = degrees(graph, notCatted)
//    val averageDegree = ds.map(x => x._1 * x._2).reduce(_ + _).toDouble / notCatted.size
//    log("average-degree-not-cut", averageDegree.toString)

//    val valedgesCutDstOrSrc = edgesCutDstOrSrc(graph, notCatted).toDouble / numberOfEdges(graph)
//    val valedgesCutDst = edgesCutDst(graph, notCatted).toDouble / numberOfEdges(graph)
//    val valedgesCutSrc = edgesCutSrc(graph, notCatted).toDouble / numberOfEdges(graph)
//    val valedgesCutDstAndSrc = edgesCutDstAndSrc(graph, notCatted).toDouble / numberOfEdges(graph)
//    val valedgesCutDstNotSrc = edgesCutDstNotSrc(graph, notCatted).toDouble / numberOfEdges(graph)
//    val valedgesCutSrcNotDst = edgesCutSrcNotDst(graph, notCatted).toDouble / numberOfEdges(graph)

//    log("normalized-cut-dst-or-src", valedgesCutDstOrSrc.toString)
//    log("normalized-cut-dst", valedgesCutDst.toString)
//    log("normalized-cut-src", valedgesCutSrc.toString)
//    log("normalized-cut-dst-and-src", valedgesCutDstAndSrc.toString)
//    log("normalized-cut-dst-not-src", valedgesCutDstNotSrc.toString)
//    log("normalized-cut-src-not-dst", valedgesCutSrcNotDst.toString)
//    log("amount-of-remote-vertices", remoteVerteices(graph).toString)

    mainObj
    //log("numberVerticesCanBeCut", numberOfVerticesCanBeCut(graph).toString)
    //    saveCSV(graph)
  }

}
