package diameter

import java.io.PrintWriter
import java.io.FileWriter
import java.io.BufferedWriter
import java.util.Queue
import java.util.concurrent._
import scala.collection.mutable.MutableList

object ErdosReniy {
  def main(args: Array[String]) {
    require(args.length >= 4, """|Wrong argument number.
                                 |Should be 4. Usage: <number of vertices> <probability> <path/to/grpah> <threds>""".stripMargin)

    val N = args(0).toInt
    val P = args(1).toDouble
    val pathToGrpah = args(2)
    val T = args(3).toInt

    val out = new PrintWriter(new BufferedWriter(new FileWriter(pathToGrpah)));

    val queue = new LinkedBlockingQueue[MutableList[(Int, Int)]]();

    val step = N / T

    new Thread(new Writer(out, T, queue)).start()

    println("step is " + step)
    for (a <- 0 until T) {
      new Thread(new Creator(a * step, a * step + step, N, P, 1000, queue)).start()
    }

  }

  class Creator(
    start: Int,
    end: Int,
    N: Int,
    P: Double,
    MaxListSize: Int,
    q: LinkedBlockingQueue[MutableList[(Int, Int)]]) extends Runnable {

    def run() {
      println(s"Creator start = $start; end = $end; N = $N; P = $P; Max = $MaxListSize")
      var list: MutableList[(Int, Int)] = MutableList[(Int, Int)]()
      for (i <- start until end) {
        if(i % 1000 == 0) println(i)
        for (j <- 0 until N) {
          if (math.random < P) {
            val p : (Int, Int) = (i ,j)
            list += p
            if (list.size >= MaxListSize) {
              q.put(list)
              list = MutableList[(Int, Int)]()
            }
          }
        }
      }
      if (!list.isEmpty) {
        q.put(list)
      }
      q.put(MutableList[(Int, Int)]())
    }
  }

  class Writer(
    out: PrintWriter,
    N: Int,
    q: LinkedBlockingQueue[MutableList[(Int, Int)]]) extends Runnable {
    def run() {
      println("Writer")
      var C = 0
      var X = 0
      while (X < N) {
        val list = q.take()
        if (list.isEmpty) {
          X = X + 1
        } else {
          C = C + 1
          for (pair <- list) {
            out.println(pair._1 + " " + pair._2)
          }
        }
        println(s"X = $X, C = $C")
      }
      out.close();

    }

  }

}


