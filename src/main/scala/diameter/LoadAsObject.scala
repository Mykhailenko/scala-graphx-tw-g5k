package diameter

import org.apache.spark.storage._
import org.apache.spark.storage._

import org.apache.spark.SparkContext._
import org.apache.spark._
import org.apache.spark.graphx._
import org.apache.spark.rdd.RDD
import java.io.File
import java.io.PrintWriter
import java.io.FileWriter

object LoadAsObject {
  def main(args: Array[String]) {

    if (args.length != 2) {
      System.err.println(
        "Usage: <objectGraph> 'result.")
      System.exit(1)
    }

    val conf = new SparkConf()
      .setAppName("Flat")
      .setJars(SparkContext.jarOfClass(this.getClass).toList)

    val sc = new SparkContext(conf)

    val edges : RDD[Edge[Any]] = sc.objectFile[Edge[Any]](args(0))
    val graph = Graph.fromEdges(edges, 0)
    
     val file = new File(args(1));
    val out = new PrintWriter(new FileWriter(file));
    
    out.println("Ola senior");
    out.println("num edges " + graph.numEdges);
    out.println("num vertices " + graph.numVertices);
    println("azazaza");
    out.close();
    
  }
}