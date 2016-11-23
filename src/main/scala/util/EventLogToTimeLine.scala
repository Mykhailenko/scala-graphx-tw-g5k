package util

import util.EventLogParser._
import spray.json._
import java.io.File
import scala.io.Source
import java.io.PrintWriter
import java.io.FileWriter
import java.util.Date
import java.text.SimpleDateFormat

object EventLogToTimeLine {

  def longFromMap(key: String, mp: Map[String, spray.json.JsValue]) = {
    val x = mp.get(key)
    if (x.isDefined) {
      x.get.asInstanceOf[JsNumber].value.toLong
    } else { 0 }
  }
  
  def filterF(map : Map[String, JsValue]) : Boolean = {
    map.values.toSet.contains(JsString("SparkListenerTaskEnd")) 
  }
  
  
  def getLong(lst : Map[String, JsValue], prop : String) : Long = {
    lst.get(prop).get.asInstanceOf[JsNumber].value.toLong
  }
  val format = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss") // '2015-02-09T04:00:00'
  
  def getDateFormatted(lst : Map[String, JsValue], prop : String) : String = {
    format.format(new Date (getLong(lst, prop)))
  }
  
  
  case class Event(id: Long, start: String, end: String, stage: Long = 0)

  object MyJsonProtocol extends DefaultJsonProtocol {
    implicit val colorFormat = jsonFormat4(Event)
  }

  import MyJsonProtocol._

  
  
  def main(args: Array[String]) {
    val pathEventLogFile = args(0)
    
	  val list = for (line <- Source.fromFile(pathEventLogFile).getLines().toList)
		  yield jsflatter(line.parseJson)
    
    
    println(list.length)
    val filtered = list.filter(filterF)
    val mapped = filtered.map(pair => {
      Event(id = getLong(pair, ".Task Info.Task ID"), start = getDateFormatted(pair, ".Task Info.Launch Time"), 
          end = getDateFormatted(pair, ".Task Info.Finish Time"), stage = getLong(pair, ".Stage ID")).toJson
      
    })
    println(mapped.size)
    println(mapped.toList)
    
//    val out = new PrintWriter(new FileWriter(args(1), true));

  }
}