name := "graphx_learning"

version := "0.1"

scalaVersion := "2.11.11"

lazy val spark = "2.2.0"
//lazy val spark = "latest.integration"

unmanagedJars in Compile += file("lib/graphframes-0.5.0-spark2.1-s_2.11.jar")

libraryDependencies ++= Seq(
  "org.apache.spark" %% "spark-core" % spark,
  "org.apache.spark" %% "spark-sql" % spark,
  "org.apache.spark" %% "spark-streaming" % spark,
  "org.apache.spark" %% "spark-graphx" % spark,
  "com.typesafe.scala-logging" %% "scala-logging" % "3.1.0",
  "com.typesafe.scala-logging" % "scala-logging-slf4j_2.11" % "2.1.2"
)

