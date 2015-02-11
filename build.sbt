name := "xErdos"

version := "1.0"

scalaVersion := "2.10.4"

libraryDependencies += "org.apache.spark" %% "spark-core" % "1.2.0"

libraryDependencies += "org.apache.spark" %% "spark-graphx" % "1.2.0"

libraryDependencies += "com.clearspring.analytics" % "stream" % "2.7.0"

libraryDependencies += "org.scalatest" % "scalatest_2.10" % "2.0"

libraryDependencies += "javax.servlet" % "javax.servlet-api" % "3.0.1"

resolvers += "Akka Repository" at "http://repo.akka.io/releases/"

