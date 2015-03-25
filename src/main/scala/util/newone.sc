package util

import sun.org.mozilla.javascript.ast.Yield

object newone {
  val qwe = List(1,1,1,2,2,3, 3, 3)               //> qwe  : List[Int] = List(1, 1, 1, 2, 2, 3, 3, 3)
  val qweq = qwe.groupBy(identity(_)).map(x => (x._1, x._2.length)).toArray
                                                  //> qweq  : Array[(Int, Int)] = Array((2,2), (1,3), (3,3))
  qwe.filter(x => math.random < 0.1)              //> res0: List[Int] = List(2)
  
  
  2 + 2                                           //> res1: Int(4) = 4
  
  
  
}