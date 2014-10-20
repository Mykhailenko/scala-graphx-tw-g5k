package ololo

import org.apache.spark.SparkContext._
import org.apache.spark._
import org.apache.spark.graphx._
import org.apache.spark.rdd.RDD
import java.io.File
import java.io.PrintWriter
import java.io.FileWriter

object Spliter {
  def main(args: Array[String]) {

    if (args.length != 2) {
      System.err.println(
        "Usage: Ololo <path_to_grpah> <path_to_result>")
      System.exit(1)
    }
    System.setProperty("spark.local.dir", "/tmp")
    System.setProperty("spark.executor.memory", "249g")

    //    val file = new File(args(1));
    //    val ola = new PrintWriter(new FileWriter(file));
    val conf = new SparkConf()
      .setAppName("Degree")
      .setSparkHome(System.getenv("SPARK_HOME"))
      .setJars(SparkContext.jarOfClass(this.getClass).toList)

    val sc = new SparkContext(conf)
    //    sc.textFile(args(0)).
    //      foreachPartition(line => {
    //        if (line.hasNext) {
    //          val fields = line.next.split(" ")
    //          fields.tail.tail.foreach(folowerId => ola.println(folowerId.toLong + " " + fields.head.toLong))
    //        }
    //      });
    val relationships: RDD[String] = sc.textFile(args(0)).flatMap(line => {
      val fields = line.split(" ")
      fields.tail.tail.map(folowerId => folowerId.toLong + " " + fields.head.toLong)
    })
    relationships.saveAsTextFile(args(1))
    //    ola.println("there are " + relationships.count() + " edges")
    //    ola.flush();
    //    ola.close();
  }
}