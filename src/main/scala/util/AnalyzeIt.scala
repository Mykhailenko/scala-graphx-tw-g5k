package util

import java.io.File

object AnalyzeIt {
  def main(args: Array[String]) {

    val expectedStructure = """| Expected root folder structure: rootFolder/
    						   |-jsonReports
          					   |-metrics
          					   |-logs
          			           |-eventLogs
          					   |-generalLog.log
    						   |-analyzed(*)
                               |""".stripMargin

    require(args.length == 1, "Should be 1 parameter with path to rootFolder\n" + expectedStructure)

    val rootPath = correctPath(args(0))

    val rootFolder = new File(rootPath)

    require(rootFolder.isDirectory(), "It is not a directory: " + args(0))
    require(!rootFolder.listFiles().find(_.getName() == "jsonReports").isDefined, "There is not 'jsonReports' folder inside root")
    require(!rootFolder.listFiles().find(_.getName() == "metrics").isDefined, "There is not 'jsonReports' folder inside root")
    require(!rootFolder.listFiles().find(_.getName() == "eventLogs").isDefined, "There is not 'jsonReports' folder inside root")

    val analyzed = new File(rootPath + "analyzed")
    if(!analyzed.exists()) analyzed.mkdir()

    // everything is prepared
    
  }

  
  
  def correctPath(path: String): String = {
    if (path.charAt(path.length() - 1) == '/') path else path + "/"
  }
}