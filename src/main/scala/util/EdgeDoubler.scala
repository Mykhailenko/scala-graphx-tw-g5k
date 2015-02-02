package util

import scala.io.Source
import java.io.PrintWriter
import java.io.FileWriter
import java.io.File

object EdgeDoubler {
  def main(args: Array[String]) {
    require(args.length == 2, "Wrong argument number. Should be 1. Usage: <path_to_grpah> <path_to_doubled>")

    val out = new PrintWriter(new FileWriter(new File(args(1))));
    var lines = Source.fromFile(args(0)).getLines
    for (line <- lines if !line.isEmpty()) {
      out.println(line)
      out.println(line.split("\\W+").reverse.mkString(" "))
    }

    out close
  }
}