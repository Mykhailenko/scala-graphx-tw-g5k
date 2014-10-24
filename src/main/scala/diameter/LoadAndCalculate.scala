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

object LoadAndCalculate {
  def main(args: Array[String]) {

    if (args.length != 2) {
      System.err.println(
        "Usage: Ololo <path_to_grpah> <path_to_result>")
      System.exit(1)
    }
    System.setProperty("spark.local.dir", "/tmp")
    System.setProperty("spark.executor.memory", "29g")
    System.setProperty("spark.eventLog.enabled", "true")
//    System.setProperty("spark.shuffle.consolidateFiles", "true")
//    System.setProperty("spark.worker.timeout", "120")
//    System.setProperty("spark.akka.frameSize", "30")

    val file = new File(args(1));
    val ola = new PrintWriter(new FileWriter(file));
    val conf = new SparkConf()
      .setAppName("Degree")
      .setSparkHome(System.getenv("SPARK_HOME"))
      .setJars(SparkContext.jarOfClass(this.getClass).toList)
    ola.println("conf was created");

    val sc = new SparkContext(conf)
    ola.println("context was created");
    //"/user/hmykhail/home/phd/tit"
    val relationships: RDD[Edge[Int]] =
      sc.textFile(args(0)).flatMap { line =>
        val fields = line.split(" ")
        fields.tail.tail.map(folowerId => Edge(folowerId.toLong, fields.head.toLong, 1))
      }

    ola.println("relationships was created : ");
    val users: RDD[(VertexId, Int)] = VertexRDD(relationships.map(edge => (edge.srcId, 0)))
    ola.println("users was created :");
    val graph: Graph[Int, Int] = Graph(users, relationships)
    ola.println("graph was created");
    //"/user/hmykhail/home/phd/rs.txt"
    ola.println("Vertices  " + graph.numVertices)
    ola.println("Edges " + graph.numEdges)

    ola.close();
  }
}