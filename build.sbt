name := "mapspark"

version := "1.0"

scalaVersion := "2.11.8"

val SPARK_VERSION = "2.2.1"

libraryDependencies ++= Seq(
  "org.apache.spark" %% "spark-core" % SPARK_VERSION,
  "org.apache.spark" %% "spark-sql" % SPARK_VERSION
)