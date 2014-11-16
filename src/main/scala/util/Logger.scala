package util

trait Logger {

  protected def log(key: String, value: String)

  def logtime(prefix: String)(body: => Unit) = {
    log(prefix + ".start", System.currentTimeMillis.toString)
    try {
      body
      log(prefix + ".result", "bien")
    } catch {
      case e: Exception => log(prefix + ".result", "exception " + e)
    }
    log(prefix + ".end", System.currentTimeMillis.toString)
  }

  def logGraphLoading = logtime("GraphLoading")_

  def logAlgorithExecution = logtime("AlgorithExecution")_

  def logResultSaving = logtime("ResultSaving")_

  def save

}