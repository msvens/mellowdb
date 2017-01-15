import sbt._
import Keys._

object Dependencies {

  //val json4sVersion = "3.5.0"

  //for testing
  val junit = "junit" % "junit" % "4.12 % "test"
  val scalatest = "org.scalatest" %% "scalatest" % "3.0.1" % "test"
  val pegdown = "org.pegdown" % "pegdown" % "1.4.2" % "test" 
  val testDeps = Seq(junit,scalatest,pegdown)

  //val json4s = "org.json4s" %% "json4s-native" % json4sVersion
  //val json4sext = "org.json4s" %% "json4s-ext" % json4sVersion

  //val jsonDeps = Seq(json4s, json4sext)


}
