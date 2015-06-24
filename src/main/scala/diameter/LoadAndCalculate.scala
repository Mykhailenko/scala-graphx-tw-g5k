package diameter

import org.apache.spark.SparkContext._
import org.apache.spark._
import org.apache.spark.graphx._
import org.apache.spark.rdd.RDD
import org.apache.spark.storage.StorageLevel
import scala.collection.immutable.Vector
import sun.security.provider.certpath.Vertex
import java.io.File
import java.io.PrintWriter
import java.io.FileWriter

object LoadAndCalculate {

    def save(graph: Graph[Int, Int], fileName: String) {
        println("Saving file to " + fileName)
        graph.edges.saveAsObjectFile(fileName)
    }

    def buildGraphFromObj(ola: PrintWriter, sc: SparkContext, fileName: String): Graph[Any, Any] = {
        ola.println("Loading graph from Object file " + fileName)
        val edges: RDD[Edge[Any]] = sc.objectFile[Edge[Any]](fileName)
        Graph.fromEdges(edges, 0)
    }

    def buildGraphFromTxt(ola: PrintWriter, sc: SparkContext, fileName: String, minEdge: Int): Graph[Int, Int] = {
        ola.println("Loading graph from Txt file " + fileName)
        GraphLoader.edgeListFile(sc, fileName, true, minEdgePartitions = minEdge)
    }

    def main(args: Array[String]) {
        println(" >> getClass  " + this.getClass.getName.stripSuffix("$"))

        if (args.length < 3) {
            System.err.println(
                "Usage: LoadAndCalculate <action> <path_to_graph> <path_to_result> [Partitions]")
            System.exit(1)
        }
        System.setProperty("spark.local.dir", "/tmp")
        var memory =
            if (args.length == 5) {
                args(4)
            } else {
                "3g"
            }

        System.setProperty("spark.executor.memory", memory)
        System.setProperty("spark.eventLog.enabled", "true")
        //    System.setProperty("spark.shuffle.consolidateFiles", "true")
        //    System.setProperty("spark.worker.timeout", "120")
        //    System.setProperty("spark.akka.frameSize", "30")

        val file = new File(args(2));
        val ola = new PrintWriter(new FileWriter(file));
        val conf = new SparkConf()
            .setAppName("Degree")
            .setSparkHome(System.getenv("SPARK_HOME"))
            .setJars(SparkContext.jarOfClass(this.getClass).toList)
        ola.println("conf was created");

        val sc = new SparkContext(conf)
        ola.println("context was created");
        val minEdge = if (args.length >= 4) {
            args(3).toInt
        } else { 16 }

        ola.println("minEdgePartitions : " + minEdge);
        ola.println("Memory : " + memory)
        var t0 = java.lang.System.currentTimeMillis();
        var graph = args(0) match {
            case "loadTxt" => buildGraphFromTxt(ola, sc, args(1), minEdge)
            case "loadObj" => buildGraphFromObj(ola, sc, args(1))
        }

        ola.println("Vertices partitions : " + graph.vertices.partitions.length)
        ola.println("Edges partitions : " + graph.edges.partitions.length)
        var t1 = java.lang.System.currentTimeMillis();
        ola.println("Graph creation took " + (t1 - t0) + " ms");

        // ola.println("Calling partitionBy with RandomVertexCut")
        // t0 = java.lang.System.currentTimeMillis();
        // graph = graph.partitionBy(PartitionStrategy.RandomVertexCut)
        // t1 = java.lang.System.currentTimeMillis();
        // ola.println("partitionBy took  " + (t1 - t0) + " ms");

        t0 = java.lang.System.currentTimeMillis();
        ola.println("Vertices  " + graph.numVertices)
        t1 = java.lang.System.currentTimeMillis();

        ola.println("Get vertices took " + (t1 - t0) + " ms");
        t0 = java.lang.System.currentTimeMillis();
        ola.println("Edges " + graph.numEdges)
        t1 = java.lang.System.currentTimeMillis();
        ola.println("Get edges took " + (t1 - t0) + " ms");
        ola.println("Max inDegrees : " + graph.inDegrees.reduce(max))
        ola.println("Max outDegrees : " + graph.outDegrees.reduce(max))
        //  save(graph, "savedGraph.obj")
        ola.close();
    }

    def max(a: (VertexId, Int), b: (VertexId, Int)): (VertexId, Int) = {
        if (a._2 > b._2) a else b
    }
}
