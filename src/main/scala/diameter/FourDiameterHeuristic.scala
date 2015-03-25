package diameter

import util.JsonLogger
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

object FourDiameterHeuristic {

  def main(args: Array[String]) {

    require(args.length == 2, "Wrong argument number. Should be 2. Usage: <path_to_grpah> <path_to_result> ")

    val sc = new SparkContext(new SparkConf()
      .setSparkHome(System.getenv("SPARK_HOME"))
      .setJars(SparkContext.jarOfClass(this.getClass).toList))

    val file = new File(args(1));
    val out = new PrintWriter(new FileWriter(file));

    var graph: Graph[Int, Int] = null
    var vertices: Array[(VertexId, Int)] = null
    var diameter: Int = 0;
    
    
    
    JsonLogger(sc) { logger =>
      import logger._

      logGraphLoading {
      
        graph = GraphLoader.edgeListFile(sc, args(0), true, edgeStorageLevel = StorageLevel.MEMORY_AND_DISK,
          vertexStorageLevel = StorageLevel.MEMORY_AND_DISK, minEdgePartitions = 20 * 12)
        vertices = graph.vertices.collect
        out println "hello world"

      }
      
      
      logPartitioning {
        
      }
      logCalculationAfterPartitioning(graph)
      
      var r1 = vertices(new Random().nextInt() % vertices.length)._1
      var rtoa1 = longestPath(r1)
      val excr1 = excentrica(r1)
      out println r1
      out println excr1._2
      out println rtoa1.length
      out close
      
      logAlgorithExecution {
//        var a1 = rtoa1(rtoa1.length - 1)
//        var a1tob1 = longestPath(a1)
//        val exca1 = excentrica(a1)
//        out println(a1 + " but " + excr1._1)
//        out println(rtoa1.length + " but " + excr1._2)
//        out close
//        var b1 = a1tob1(a1tob1.length - 1)
//        var r2 = a1tob1(a1tob1.length / 2)
//        
//        var r2toa2 = longestPath(r2)
//        var a2 = r2toa2(r2toa2.length - 1)
//        var a2tob2 = longestPath(a2)
//        var b2 = a2tob2(a2tob2.length - 1)
//        var u = a2tob2(a2tob2.length / 2)
//        var lowerb = Math.max(a1tob1.length, a2tob2.length)
//        out println "r1 = " + r1
//        out println "a1 = " + a1
//        out println "b1 = " + b1
//        out println "r2 = " + r2
//        out println "a2 = " + a2
//        out println "b2 = " + b2
//        out println "u = " + u
//        out println "lowerb " + lowerb
//        out flush
      }

      logResultSaving {

      }
      out.close()
    }

    def printPath(path: List[VertexId]) = {
      out println "path len = " + path.length
      out println "start"
      for (p <- path) {
        out println p
      }
      out println "end"
    }

    
    def longestPath(sourceId: VertexId): List[VertexId] = {
      val initialGraph: Graph[(Long, List[VertexId]), Int] = graph.mapVertices((id, _) => if (id == sourceId) (0, List[VertexId](sourceId)) else (Long.MaxValue, List[VertexId]()))
      val shortestPathGraph = initialGraph.pregel((Long.MaxValue, List[VertexId]()))(

        (id, dist, newDist) => if (dist._1 < newDist._1) dist else newDist, // Vertex Program

        triplet => { // Send Message
          if (triplet.srcAttr._1 < triplet.dstAttr._1 - 1) {
            Iterator((triplet.dstId, (triplet.srcAttr._1 + 1, triplet.dstId +: triplet.srcAttr._2)))
          } else {
            Iterator.empty
          }
        },

        (a, b) => if (a._1 < b._1) a else b)
      val ret = shortestPathGraph.vertices.filter(v => v._2._1 != Long.MaxValue).collect.sortBy(v => v._2._1).reverse.head._2._2.reverse
      val ecc = excentrica(sourceId)
      require(1 == 2)
      require(ret(ret.length - 1 ) != ecc._1)
      ret
    }
    def fivePaths(sourceId: VertexId): Array[List[VertexId]] = {
      val initialGraph: Graph[(Long, List[VertexId]), Int] = graph.mapVertices((id, _) => if (id == sourceId) (0, List[VertexId](sourceId)) else (Long.MaxValue, List[VertexId]()))
      val shortestPathGraph = initialGraph.pregel((Long.MaxValue, List[VertexId]()))(

        (id, dist, newDist) => if (dist._1 < newDist._1) dist else newDist, // Vertex Program

        triplet => { // Send Message
          if (triplet.srcAttr._1 < triplet.dstAttr._1 - 1) {
            Iterator((triplet.dstId, (triplet.srcAttr._1 + 1, triplet.dstId +: triplet.srcAttr._2)))
          } else {
            Iterator.empty
          }
        },

        (a, b) => if (a._1 < b._1) a else b)
      shortestPathGraph.vertices.filter(v => v._2._1 != Long.MaxValue).collect.sortBy(v => v._2._1).reverse.map(x => x._2._2).take(5)

    }
    def excentrica(sourceId: VertexId): (VertexId, Double) = {
      val initialGraph = graph.mapVertices((id, _) => if (id == sourceId) 0 else Double.PositiveInfinity)
      val shortestPathGraph = initialGraph.pregel(Double.PositiveInfinity)(

        (id, dist, newDist) => math.min(dist, newDist), // Vertex Program

        triplet => { // Send Message
          if (triplet.srcAttr + 1 < triplet.dstAttr) {
            Iterator((triplet.dstId, triplet.srcAttr + 1))
          } else {
            Iterator.empty
          }
        },

        (a, b) => math.min(a, b) // Merge Message
        )
      shortestPathGraph.vertices.filter(distance => distance._2 != Double.PositiveInfinity).collect.sortBy(v => v._2).reverse.head
    }

    def pathes(sourceId: VertexId): Array[(VertexId, (Long, List[VertexId]))] = {
      val initialGraph: Graph[(Long, List[VertexId]), Int] = graph.mapVertices((id, _) => if (id == sourceId) (0, List[VertexId](sourceId)) else (Long.MaxValue, List[VertexId]()))
      val shortestPathGraph = initialGraph.pregel((Long.MaxValue, List[VertexId]()))(

        (id, dist, newDist) => if (dist._1 < newDist._1) dist else newDist, // Vertex Program

        triplet => { // Send Message
          if (triplet.srcAttr._1 < triplet.dstAttr._1 - 1) {
            Iterator((triplet.dstId, (triplet.srcAttr._1 + 1, triplet.dstId +: triplet.srcAttr._2)))
          } else {
            Iterator.empty
          }
        },

        (a, b) => if (a._1 < b._1) a else b)
      shortestPathGraph.vertices.filter(v => v._1 != Long.MaxValue).collect.sortBy(v => v._2._1).reverse

    }

  }
}

