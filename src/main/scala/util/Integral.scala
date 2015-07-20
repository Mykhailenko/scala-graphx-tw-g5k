package util

import java.io.File
import scala.io.Source
import java.io.PrintWriter
import java.io.FileWriter
import java.util.regex.Pattern
import spray.json._
import util.EventLogParser._

object Integral {
  def main(args: Array[String]) {
    val file = new File(args(0))

    val resultFile = new File(args(1))
    
    val triplets = Source.fromFile(file)
      .getLines()
      .map(_.parseJson)
      .map(x => {
       if(!x.asJsObject.getFields("Task Metrics").isEmpty){
         val vc = x.asJsObject.getFields("Task Info").head.asJsObject
         val s = vc.getFields("Launch Time").head.asInstanceOf[JsNumber].value.toInt
         val f = vc.getFields("Finish Time").head.asInstanceOf[JsNumber].value.toInt
         val c = x.asJsObject.getFields("Task Metrics").head.asJsObject
         val mem = if(!c.getFields("Updated Blocks").isEmpty){
           val ar = c.getFields("Updated Blocks").head.asInstanceOf[JsArray]
           val col = for(a <- ar.elements) yield {
             if(!a.asJsObject.getFields("Status").isEmpty){
               a.asJsObject.getFields("Status").head.asJsObject.getFields("Memory Size").head.asInstanceOf[JsNumber].value.toInt / 1024
             }else{
               0
             }
           }
           Some(col.sum)
         }else{
           None
         }
         if(mem.isDefined){
        	 Some((s, f, mem))
         }else{
           None
         }
       }else{
         None
       } 
      }).toList.filter(x => x != None).map(x => (x.get._1, x.get._2, x.get._3.get))
      
    println("triplets.length = " + triplets.length)
    triplets.foreach(x => if (x != (0, 0, 0)) println(x))
    val startTime = triplets.map(_._1).min
    val endTime = triplets.map(_._2).max
    println(s"startTime = $startTime; endTime = $endTime ")
    println("diff = " + (endTime - startTime))
    val data = for(i <- startTime to (endTime, 100)) yield {
      val v = triplets.filter(x => x._1 <= i && i <= x._2).map(_._3).sum
      ((i - startTime).toInt / 100, v.toInt)
    }
//    
    val content = data.map(x => x._1 + ", " + x._2).mkString("\n")
//    
    val out = new PrintWriter(new FileWriter(resultFile))
    out.print(content)
    out.close
    
  }
}