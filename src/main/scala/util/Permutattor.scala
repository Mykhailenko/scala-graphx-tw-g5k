package util

import scala.io.Source
import java.io.File
import java.io.PrintWriter
import java.io.FileWriter

object Permutattor {
  def main(args: Array[String]) {
    require(args.length == 2, "Wrong argument number. Should be 1. Usage: <path_to_grpah> <path_to_permutatted>")

    var lines = Source.fromFile(args(0)).getLines
    var lis = Set[Int]()
    while (lines.hasNext) {
      var line = lines.next

      var v = line.split("\\W+")
      if(v.length != 2){
        return
      }
      if (v.length == 2) {
        var vv = v.map(Integer.parseInt)
        lis = lis + vv(0) + vv(1)
      }
    }
    var sss = lis.toList
    var mmm = Map[Int, Int]()
    def remove(num: Int, list: List[Int]) = (list take num) ++ (list drop (num + 1))
    var rand = new scala.util.Random
    for (x <- lis) {
      if (!mmm.contains(x)) {
        var index = rand.nextInt(sss.size)
        mmm = mmm + (x -> sss(index))
        sss = remove(index, sss)
      }
    }

    lines = Source.fromFile(args(0)).getLines

    val out = new PrintWriter(new FileWriter(new File(args(1))));

    for (line <- lines) {
      out.println(line.split("\\W+").filter(!_.isEmpty()).map(Integer.parseInt).toList.map(x => mmm.getOrElse(x, 0)).mkString("\t"))
    }

    out close

  }
}