package ololo

import org.apache.spark.SparkContext._
import org.apache.spark._
import org.apache.spark.graphx._
import org.apache.spark.rdd.RDD
import java.io.File
import java.io.PrintWriter
import java.io.FileWriter

object Flatter {
  def main(args: Array[String]) {

    if (args.length != 2) {
      System.err.println(
        "Usage: Ololo <graph> <flatGraph> ")
      System.exit(1)
    }

    System.setProperty("spark.local.dir", "/tmp")
    System.setProperty("spark.executor.memory", "29g")

    //    val file = new File(args(1));
    //    val out = new PrintWriter(new FileWriter(file));
    val conf = new SparkConf()
      .setAppName("Flat")
      .setSparkHome(System.getenv("SPARK_HOME"))
      .setJars(SparkContext.jarOfClass(this.getClass).toList)

    val sc = new SparkContext(conf)

    val relationships: RDD[String] = sc.textFile(args(0)).flatMap(line => {
      val fields = line.split(" ")
      fields.tail.tail.map(folowerId => folowerId.toLong + " " + fields.head.toLong)
    })

    relationships.coalesce(1, true).saveAsTextFile(args(1))
    //    val arr: Array[String] = relationships.collect
    //    for (line <- arr) {
    //      out.println(line)
    //    }
    //
    //    out.flush();
    //    out.close();
  }
}