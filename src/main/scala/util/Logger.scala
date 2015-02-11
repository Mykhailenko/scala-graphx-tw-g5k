package util

import org.apache.spark.graphx.Graph

trait Logger {

  protected def log(key: String, value: String)

  protected def logtime(prefix: String)(body: => Unit) = {
    val start = System.currentTimeMillis
    log(prefix + ".start", start.toString)
    try {
      body
      log(prefix + ".result", "bien")
    } catch {
      case e: Exception => log(prefix + ".result", "exception " + e)
    }
    val end = System.currentTimeMillis
    log(prefix + ".end", end.toString)
    log(prefix + ".time", (end - start).toString)
  }

  def logGraphLoading = logtime("GraphLoading")_

  def logAlgorithExecution = logtime("AlgorithExecution")_

  def logResultSaving = logtime("ResultSaving")_

  def logPartitioning(body: => Unit)

  def logCalculationAfterPartitioning(graph: Graph[Int, Int])

  def save

}
