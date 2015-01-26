name := "scala-graphx-tw-g5k"

version := "1.0"

scalaVersion := "2.10.4"

autoScalaLibrary := true


libraryDependencies += "org.apache.spark" %% "spark-core" % "1.1.0"

libraryDependencies += "org.apache.spark" %% "spark-graphx" % "1.1.0"

libraryDependencies += "io.spray" %%  "spray-json" % "1.3.1"

mainClass in Compile := Some("util.RaportMaker")

resolvers += "Akka Repository" at "http://repo.akka.io/releases/"

