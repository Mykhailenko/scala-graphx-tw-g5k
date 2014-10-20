package ololo

import org.apache.spark.SparkContext._
import org.apache.spark._
import org.apache.spark.graphx._
import org.apache.spark.rdd.RDD
import java.io.File
import java.io.PrintWriter
import java.io.FileWriter

object Facebook2 {

  def excentrica(graph: Graph[Int, Int], sourceId: VertexId): Double = {
    val initialGraph = graph.mapVertices((id, _) => if (id == sourceId) 0 else Double.PositiveInfinity)
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
  def main(args: Array[String]) {

    if (args.length != 2) {
      System.err.println(
        "Usage: Ololo <path_to_grpah> <path_to_result>")
      System.exit(1)
    }
    //    System.setProperty("spark.local.dir", "/data/hmykhailenko_595024")
    //    System.setProperty("spark.executor.memory", "254g")

    val file = new File(args(1));
    val ola = new PrintWriter(new FileWriter(file));
    val conf = new SparkConf()
      .setAppName("Degree")
      .setSparkHome(System.getenv("SPARK_HOME"))
      .setJars(SparkContext.jarOfClass(this.getClass).toList)
    ola.println("!conf was created");

    val sc = new SparkContext(conf)
    ola.println("!context was created");

    val graph = GraphLoader.edgeListFile(sc, args(0))

    ola.println("graph was created");
    //"/user/hmykhail/home/phd/rs.txt"
    ola.println("Vertices  " + graph.numVertices)
    ola.println("Edges " + graph.numEdges)
    
    val plaineVertex: Array[(VertexId, Int)] = graph.vertices.collect;
    ola.println(plaineVertex.length)
    for ((sourceId, _) <- plaineVertex) {

      ola.println(sourceId + " excentric= " + excentrica(graph, sourceId))
    }
    //    ola.println("Ola! " + graph.inDegrees.count)

    ola.close();

  }
}