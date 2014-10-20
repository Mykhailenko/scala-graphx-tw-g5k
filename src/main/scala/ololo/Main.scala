package ololo

import org.apache.spark.SparkContext._
import org.apache.spark._
import org.apache.spark.graphx._
import org.apache.spark.rdd.RDD

object Main {
  def main(args: Array[String]) {

    if (args.length == 0) {
      System.err.println(
        "Usage: Ololo <master> [followers] [users] ")
      System.exit(1)
    }

    val pathToFollowers = if (args.length > 1) args(1).toString else "/root/followers.txt"
    val pathToUsers = if (args.length > 2) args(2).toString else "/root/users.txt"

    val conf = new SparkConf()
      .setAppName("Experiment avec la partiotioner")
      .setSparkHome(System.getenv("SPARK_HOME"))
      .setMaster(args(0))
      .setJars(SparkContext.jarOfClass(this.getClass).toList)

    val sc = new SparkContext(conf)

    val graph = GraphLoader.edgeListFile(sc, pathToFollowers, true).partitionBy(OloloVertexCut)

    // Run PageRank
    val ranks = graph.pageRank(0.0001).vertices

    // Join the ranks with the usernames
    val users = sc.textFile(pathToUsers).map { line =>
      val fields = line.split(",")
      (fields(0).toLong, fields(1))
    }

    val ranksByUsername = users.join(ranks).map {
      case (id, (username, rank)) => (username, rank)
    }

    // Print the result
    println(ranksByUsername.collect().mkString("\n"))
  }
}

case object OloloVertexCut extends PartitionStrategy {
  override def getPartition(src: VertexId, dst: VertexId, numParts: PartitionID): PartitionID = {
    math.abs((src, dst).hashCode()) % numParts
  }
}
