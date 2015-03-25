package diameter

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

object Canonical {

  def main(args: Array[String]) {

    val numberOfArguments = 3
    require(args.length == numberOfArguments, s"""Wrong argument number. Should be $numberOfArguments . 
                                                 |Usage: <path_to_grpah> <partiotioner_name> <minEdgePartitions> """.stripMargin)

    var graph: Graph[Int, Int] = null
    val nameOfGraph = args(0).substring(args(0).lastIndexOf("/") + 1)
    val partitionerName = args(1)
    val minEdgePartitions = args(2).toInt

    val sc = new SparkContext(new SparkConf()
      .setSparkHome(System.getenv("SPARK_HOME"))
      .setAppName(s" partitioning | $nameOfGraph | $partitionerName | $minEdgePartitions parts ")
      .setJars(SparkContext.jarOfClass(this.getClass).toList))

    graph = GraphLoader.edgeListFile(sc, args(0), false, edgeStorageLevel = StorageLevel.MEMORY_AND_DISK,
      vertexStorageLevel = StorageLevel.MEMORY_AND_DISK, minEdgePartitions = minEdgePartitions)
    graph = graph.partitionBy(PartitionStrategy.fromString(partitionerName))

    var initEdges = graph.edges.mapValues(x => PartitionStrategy.RandomVertexCut.getPartition(x.srcId, x.dstId, minEdgePartitions))

    val neigbours = graph.collectNeighborIds(EdgeDirection.Out)

    val cc = graph.edges.partitionsRDD.collect
    for(ziga <- cc){
      println("partition id " + ziga._1)
      val ep = ziga._2
      for(i <- 0 until ep.srcIds.length){
        println("ep.srcIds(i) = " + ep.srcIds(i) + "; attr = " + ep.data(i) +"; ep.dstIds(i) = " + ep.dstIds(i))
      }
      println("then VertexЕPartition")
      
      for(v <- ep.vertices.values){
        println("v = " + v)
      }
        
    }
    println("wasted day")
//    val rr = cc.mapPartitions({ iter =>
//      
//      Iterator(iter)
//    } 
//    , preservesPartitioning = true)

    println("start of repartition:")
    pringEdges(initEdges)
    //    for (v <- graph.vertices.collect) {
    //      println("v = " + v._1)
    //      println("Dominant = " + getDominantPartition(v._1, initEdges))
    //      if (isInternal(v._1, initEdges)) {
    //        println("it is internal vertex")
    //      } else {
    //        println("Selected edge: ")
    //        printEdge(selectEdge(v._1, initEdges))
    //      }
    //    }

    
    
    for (v <- graph.vertices.collect) {
      //      println("v = " + v._1)
      //      for (e <- getEdges(v._1)) {
      //        printEdge(e)
      //      }
      if (!isInternal(v._1, initEdges)) {
        val candidates = getCandidates(v._1)
        val selectedEdge = selectEdge(v._1, initEdges)
        var notSwapped = true;
        for (partner <- candidates if !isInternal(partner, initEdges) && notSwapped) {
          val selfPartition = getDominantPartition(v._1, initEdges)
          val partnerPartition = getDominantPartition(partner, initEdges)
          val partnerEdge = getEdge(partner, selfPartition, initEdges)
          if (selfPartition != partnerPartition) {

            if (partnerEdge != null) {
              //               then swap partnerEdge and selectedEdge
              initEdges = initEdges.mapValues(x => {
                if (sameSrcAndDst(x, selectedEdge)) {
                  partnerEdge.attr
                } else if (sameSrcAndDst(x, partnerEdge)) {
                  selectedEdge.attr
                } else {
                  x.attr
                }
              })
              println("just swapped ")
              printEdge(selectedEdge)
              printEdge(partnerEdge)
              pringEdges(initEdges)
              notSwapped = false
            }

          }
        }
      }
    }

    println("end of repartition:")

    def pringEdges(edges: EdgeRDD[PartitionID, Int]): Unit = {
      for (v <- graph.vertices.collect) {
        println("v = " + v._1)
        for (e <- getEdges(v._1, edges)) {
          printEdge(e)
        }
      }
    }

    def printEdge(edge: Edge[PartitionID]): Unit = {
      println("edge.srcId = " + edge.srcId + "; edge.attr = " + edge.attr + "; edge.dstId = " + edge.dstId)
    }

    def sameSrcAndDst(a: Edge[PartitionID], b: Edge[PartitionID]): Boolean = {
      println(a)
      println(b)
      val res = a.srcId == b.srcId && a.dstId == b.dstId
      println("res = " + res)
      res
    }

    def getEdge(vertex: VertexId, partition: PartitionID, edges: EdgeRDD[PartitionID, Int]): Edge[PartitionID] = {
      getEdges(vertex, edges).filter(x => x.attr == partition).head
    }

    def isInternal(vertex: VertexId, edges: EdgeRDD[PartitionID, Int]): Boolean = {
      val edgs = getEdges(vertex, edges)
      if (!edgs.isEmpty) {
        edgs.map(x => x.attr).toSet.size <= 1
      } else {
        true
      }
    }

    def getEdges(vertex: VertexId, edges: EdgeRDD[PartitionID, Int]): Array[Edge[PartitionID]] = {
      edges.filter(v => v.srcId == vertex).collect
    }

    def getDominantPartition(vertex: VertexId, edges: EdgeRDD[PartitionID, Int]): PartitionID = {
      getEdges(vertex, edges).groupBy(x => x.attr).toList.sortWith((a, b) => a._2.length < b._2.length).last._1
    }

    def selectEdge(vertex: VertexId, edges: EdgeRDD[PartitionID, Int]): Edge[PartitionID] = {
      val edgs = getEdges(vertex, edges)
      val sortedByPartitionID = edgs.groupBy(x => x.attr).toList.sortWith((a, b) => a._2.length < b._2.length)
      val partitionIDWithMinimalCardinality = sortedByPartitionID.head._1
      val result = sortedByPartitionID.head._2.head
      result
    }
    def neighbours(vertex: VertexId): Array[VertexId] = {
      val neighbourOfV = neigbours.filter(v => v._1 == vertex).collect
      if (!neighbourOfV.isEmpty) {
        neighbourOfV.head._2
      } else {
        Array.empty
      }
    }
    def randomVertex(): VertexId = {
      0
    }
    def getCandidates(vertex: VertexId): Array[VertexId] = {
      neighbours(vertex) //:+ randomVertex()
    }

  }
}