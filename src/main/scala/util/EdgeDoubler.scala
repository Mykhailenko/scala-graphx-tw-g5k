package util

import scala.io.Source
import java.io.PrintWriter
import java.io.FileWriter
import java.io.File

object EdgeDoubler {
  def main(args: Array[String]) {
    require(args.length >= 2, "Wrong argument number. Should be 1. Usage: <path_to_grpah> <path_to_doubled>")
    val out = new PrintWriter(new FileWriter(new File(args(1))));
    var lines = Source.fromFile(args(0)).getLines
    val prob =
    if (args.length > 2) args(2).toDouble else 1.0
    var i = 0;
    val r = scala.util.Random
//    println("ww")
    for (line <- lines if !line.isEmpty()) {
//      println(line)
      val a = line.split("\\W+")
      require(a.length == 2, "after spliting length is not 2 but: " + a.length)
      out.println(line)
      if(r.nextFloat <= prob) out.println(a.reverse.mkString(" "))
      i = i + 1
      if(i % 1000 == 0){
        println(i)
      }
    }

    out close
  }
}
