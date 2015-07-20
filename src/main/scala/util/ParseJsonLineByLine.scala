package util

import util.EventLogParser._
import spray.json._
import java.io.File
import scala.io.Source

object ParseJsonLineByLine {
  def main(args: Array[String]) {
    
    val file = new File(args(0))

    val lines = Source.fromFile(file).getLines().toList

    var i = 0
    println("before")
    lines.foreach{line =>
      println(s"try to parse $i line")
      i = i + 1
      jsflatter(line.parseJson)
    }
    println("afta")
      
  }
}