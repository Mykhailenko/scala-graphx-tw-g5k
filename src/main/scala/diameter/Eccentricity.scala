package diameter

import org.apache.spark.SparkContext._
import org.apache.spark._
import org.apache.spark.storage._
import org.apache.spark.graphx._
import org.apache.spark.rdd.RDD
import scala.collection.immutable.Vector
import sun.security.provider.certpath.Vertex
import java.io.File
import java.io.PrintWriter
import java.io.FileWriter

object Eccentricity {

  def main(args: Array[String]) {

    if (args.length != 2 && args.length != 3) {
      System.err.println(
        "Usage: <path_to_grpah> <path_to_result> <count_of_vertices=1>")
      System.exit(1)
    }

    val count_of_vertices = if (args.length == 3) args(2).toInt else 1

    val conf = new SparkConf()
      .setAppName("Experiment avec la Twitter")
      .setSparkHome(System.getenv("SPARK_HOME"))
      .setJars(SparkContext.jarOfClass(this.getClass).toList)

    val sc = new SparkContext(conf)

    val graph = GraphLoader.edgeListFile(sc, args(0), true, edgeStorageLevel = StorageLevel.MEMORY_AND_DISK,
      vertexStorageLevel = StorageLevel.MEMORY_AND_DISK, minEdgePartitions = 1000).cache()

    val plaineVertex: Array[(VertexId, Int)] = graph.vertices.take(count_of_vertices)

    def excentrica(sourceId: VertexId): Double = {

      val initialGraph = graph.mapVertices((id, _) => if (id == sourceId) 0.0 else Double.PositiveInfinity)
      val sssp = initialGraph.pregel(Double.PositiveInfinity)(
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

      sssp.vertices.filter(distance => distance._2 != Double.PositiveInfinity).collect.sortBy(v => v._2).reverse.head._2
    }

    val file = new File(args(1));
    val out = new PrintWriter(new FileWriter(file));
    for ((sourceId, _) <- plaineVertex) {

      out.println(sourceId + " excentric= " + excentrica(sourceId))
    }
    out.println("Vertices  " + graph.numVertices)
    out.println("Edges " + graph.numEdges)
    out.close();
  }
}
