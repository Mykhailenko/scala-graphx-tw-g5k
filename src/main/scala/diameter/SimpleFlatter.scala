package diameter

import java.io.BufferedReader
import java.io.FileReader
import java.io.BufferedWriter
import java.io.FileWriter
import java.io.PrintWriter

object SimpleFlatter {
  def main(args: Array[String]) {
    require(args.length == 2, "Usage: <graph> <flatGraph> [how many edges use in percentage from 0.0 till 1.0]")
    val br = new BufferedReader(new FileReader(args(0)))
    val bw = new PrintWriter(new BufferedWriter(new FileWriter(args(1))))
    var line = br.readLine()
    var buff = 0
    var total = 0
    while (line != null) {
      // process the line.
      val ar = line.split(" ")
      val dest = ar(0)
      for(i <- 2 until ar.length) yield {
        bw.println(ar(i) + " " + dest)
      }
      total += ar.length - 2
      buff += ar.length - 2
      if(buff > 100000){
        println((total / 100000))
        buff = 0
        bw.flush()
      }
      line = br.readLine()
    }
    br.close()
    bw.close()
  }
}