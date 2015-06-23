package util

import java.io.File
import java.io.PrintWriter
import java.io.FileWriter

object EventLogFileAnalyzer {
  def main(args: Array[String]) {
    require(args.length == 2, "Two arguments are required: <path/to/a/file/EVENT_LOG_1> and <minimum stage duration in ms>" )

    val file = new File(args(0))

    require(file.isFile(), "It is not a file.")

    val parent = file.getParent()

    EventLogParser.main(Array(parent, "false"))

    TaskDurationComparator.main(Array(args(0), parent + "/tasks", parent + "/stages", args(1)))
    
    val aout = new PrintWriter(new FileWriter(parent + "/tasksandstages"));
    aout.print(template)
    aout.close
    
  }
  
  val template = """|set style fill transparent solid 0.85
                    |set term pdf
                    |set title "Tasks/stages execution time"
		  			|set xlabel "Task/stages IDs"
		  			|set ylabel "Time (ms)"
		  			|set output "tasksandstages.pdf"
                    |plot  'tasks' using 1:2:2:3:3 lw 0.5 with candlesticks title 'Tasks', 'stages' using 1:2:2:3:3 lw 0.5 with candlesticks title 'Stages'""".stripMargin

}