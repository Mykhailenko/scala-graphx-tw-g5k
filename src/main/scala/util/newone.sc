package util

import sun.org.mozilla.javascript.ast.Yield

object newone {
  val qwe = List(1, 1, 1, 2, 2, 3, 3, 3)          //> qwe  : List[Int] = List(1, 1, 1, 2, 2, 3, 3, 3)
  val qweq = qwe.groupBy(identity(_)).map(x => (x._1, x._2.length)).toArray
                                                  //> qweq  : Array[(Int, Int)] = Array((2,2), (1,3), (3,3))
  qwe.filter(x => math.random < 0.1)              //> res0: List[Int] = List()

  2 + 2                                           //> res1: Int(4) = 4

  def zip(a: List[List[Int]], b: List[List[Int]], default: List[Int] = List(0, 0, 0)): List[List[Int]] = {
    val maxLenght = List(a.length, b.length).max

    def get(lst: List[List[Int]], index: Int): List[Int] = {
      if (0 <= index && index < lst.length) lst(index) else default
    }

    val res = for (i <- 0 until maxLenght) yield get(a, i) ++ get(b, i)
    res.toList

  }                                               //> zip: (a: List[List[Int]], b: List[List[Int]], default: List[Int])List[List[I
                                                  //| nt]]

  val a = List(List(1, 2), List(2, 2), List(4, 4), List(5, 4))
                                                  //> a  : List[List[Int]] = List(List(1, 2), List(2, 2), List(4, 4), List(5, 4))
  val b = List(List(2, 10), List(3, 4), List(5, 3))
                                                  //> b  : List[List[Int]] = List(List(2, 10), List(3, 4), List(5, 3))
  val d = List(0, 0)                              //> d  : List[Int] = List(0, 0)
  zip(a, b)                                       //> res2: List[List[Int]] = List(List(1, 2, 2, 10), List(2, 2, 3, 4), List(4, 4,
                                                  //|  5, 3), List(5, 4, 0, 0, 0))
}