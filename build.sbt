name := "DECT"
 
version := "1.9"
  
scalaVersion := "2.10.4"

scalacOptions ++= Seq("-unchecked", "-deprecation", "-feature")

libraryDependencies += "org.rogach" %% "scallop" % "0.9.5"
libraryDependencies += "com.typesafe" % "config" % "1.3.0"
libraryDependencies += "org.apache.spark" %% "spark-core" % "1.3.1"
