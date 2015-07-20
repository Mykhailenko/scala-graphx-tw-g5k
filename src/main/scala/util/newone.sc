package util

import sun.org.mozilla.javascript.ast.Yield

object newone {
	val a = (0, 10, 1)                        //> a  : (Int, Int, Int) = (0,10,1)
	val b = (5, 15, 2)                        //> b  : (Int, Int, Int) = (5,15,2)
	val c = (11, 16, 1)                       //> c  : (Int, Int, Int) = (11,16,1)
	val ls = Array(a,b,c)                     //> ls  : Array[(Int, Int, Int)] = Array((0,10,1), (5,15,2), (11,16,1))
	val st = ls.flatMap(x => List(x._1, x._2)).toSet.toList.sorted
                                                  //> st  : List[Int] = List(0, 5, 10, 11, 15, 16)
	for(i <- st) yield {
	  val v = ls.filter(x => x._1 <= i && i <= x._2).map(_._3).sum
	  (i, v)
	}                                         //> res0: List[(Int, Int)] = List((0,1), (5,3), (10,3), (11,3), (15,3), (16,1))
	
	  val text = "param-1000-partitionandconnectedcommunity-twitter_5.txt-randomvertexcut-7-partitions-7-cores-1436273175365"
                                                  //> text  : String = param-1000-partitionandconnectedcommunity-twitter_5.txt-ran
                                                  //| domvertexcut-7-partitions-7-cores-1436273175365
	
	  val pat = "^param-(\\S+)-partitionand(\\S+)-(\\S+)_(\\S+).txt-(\\S+)-(\\S+)-partitions-(\\S+)-cores-(\\S+)$"
                                                  //> pat  : String = ^param-(\S+)-partitionand(\S+)-(\S+)_(\S+).txt-(\S+)-(\S+)-p
                                                  //| artitions-(\S+)-cores-(\S+)$
		val s = "ftp://anon:bah@111.222.333.444:9999/path1/path2/";
                                                  //> s  : String = ftp://anon:bah@111.222.333.444:9999/path1/path2/
		val pattern = "^ftp://(\\S+):(\\S+)@(\\S+):(\\d+)(\\S+)$";
                                                  //> pattern  : String = ^ftp://(\S+):(\S+)@(\S+):(\d+)(\S+)$
		import java.util.regex.Pattern
		val p = Pattern.compile(pat);     //> p  : java.util.regex.Pattern = ^param-(\S+)-partitionand(\S+)-(\S+)_(\S+).tx
                                                  //| t-(\S+)-(\S+)-partitions-(\S+)-cores-(\S+)$
		
		val m = p.matcher(text);          //> m  : java.util.regex.Matcher = java.util.regex.Matcher[pattern=^param-(\S+)-
                                                  //| partitionand(\S+)-(\S+)_(\S+).txt-(\S+)-(\S+)-partitions-(\S+)-cores-(\S+)$ 
                                                  //| region=0,106 lastmatch=]
		
		if (m.matches()) {
		  for(i <- 0 until m.groupCount()){
		    println(s"$i:" + m.group(i))
		  }
			System.out.println("so!!! if I call m.group() it'll throw IllegalStateException !");
		} else {
			System.out.println("so if I call m.group() it'll throw IllegalStateException !");
		}                                 //> 0:param-1000-partitionandconnectedcommunity-twitter_5.txt-randomvertexcut-7
                                                  //| -partitions-7-cores-1436273175365
                                                  //| 1:1000
                                                  //| 2:connectedcommunity
                                                  //| 3:twitter
                                                  //| 4:5
                                                  //| 5:randomvertexcut
                                                  //| 6:7
                                                  //| 7:7
                                                  //| so!!! if I call m.group() it'll throw IllegalStateException !
		
}