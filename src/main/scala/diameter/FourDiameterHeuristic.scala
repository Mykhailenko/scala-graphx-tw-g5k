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

object FourDiameterHeuristic {

  def main(args: Array[String]) {

    if (args.length != 2) {
      System.err.println(
        "Wrong argument number. Should be 2. Usage: <path_to_grpah> <path_to_result>")
      System.exit(1)
    }
    val sc = new SparkContext(new SparkConf()
      .setSparkHome(System.getenv("SPARK_HOME"))
      .setJars(SparkContext.jarOfClass(this.getClass).toList))
    val file = new File(args(1));
    val out = new PrintWriter(new FileWriter(file));

    var graph: Graph[Int, Int] = null
    var plaineVertex: Array[(VertexId, Int)] = null
    var result : Array[(VertexId, Double)] = null

    JsonLogger(sc) { logger =>
      import logger._

      logGraphLoading {
        graph = GraphLoader.edgeListFile(sc, args(0), true, 8)
        plaineVertex = graph.vertices.take(5)
      }
      logAlgorithExecution {
        result = for ((sourceId, _) <- plaineVertex) yield excentrica(sourceId) 
      }
       
      logResultSaving{
        for((sourceId, excentrica) <- result){
    	  out.println(sourceId + " excentric= " + excentrica)
    	  out.close();
        }
      }
    }

    def excentrica(sourceId: VertexId): (VertexId, Double) = {
      val initialGraph = graph.mapVertices((id, _) => if (id == sourceId) 0 else Double.PositiveInfinity)
      val shortestPathGraph = initialGraph.pregel(Double.PositiveInfinity)(

        (id, dist, newDist) => math.min(dist, newDist), // Vertex Program

        triplet => { // Send Message
          if (triplet.srcAttr + triplet.attr < triplet.dstAttr) {
            Iterator((triplet.dstId, triplet.srcAttr + triplet.attr))
          } else {
            Iterator.empty
          }
        },

        (a, b) => math.min(a, b) // Merge Message
        )
      shortestPathGraph.vertices.filter(distance => distance._2 != Double.PositiveInfinity).collect.sortBy(v => v._2).reverse.head
    }

  }
}