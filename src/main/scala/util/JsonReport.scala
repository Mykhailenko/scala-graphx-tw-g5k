package util

import java.sql.Date
import scala.io.Source

import spray.json._
import DefaultJsonProtocol._

class JsonReport (path : String) extends Report {

  val map = scala.io.Source.fromFile(path).mkString.parseJson.convertTo[Map[String, String]]

  private def getLong(key : String) : Long = {
    (map getOrElse (key, "0") ).toLong
  }
  private def get(key : String) = {
    map getOrElse (key, "")
  }

  def graphLoadingStart: Date = new Date(getLong("GraphLoading.start"))
  def graphLoadingEnd: Date = new Date(getLong("GraphLoading.end"))
  def graphLoadingTime: Long = getLong("GraphLoading.end") - getLong("GraphLoading.start")

  def algorithExecutionStart: Date = new Date(getLong("AlgorithExecution.start"))
  def algorithExecutionEnd: Date = new Date(getLong("AlgorithExecution.end"))
  def algorithExecutionTime: Long = getLong("AlgorithExecution.end") - getLong("AlgorithExecution.start")

  def resultSavingStart: Date = new Date(getLong("ResultSaving.start"))
  def resultSavingEnd: Date = new Date(getLong("ResultSaving.end"))
  def resultSavingTime: Long = getLong("ResultSaving.end") - getLong("ResultSaving.start")

  def master : String = get("master")

  def wholeTime : Long = graphLoadingTime + algorithExecutionTime + resultSavingTime
  def name : String = get("name")
}
