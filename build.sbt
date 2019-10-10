name := "json-schema-scala"

version := "0.1"

scalaVersion := "2.11.11"

libraryDependencies ++= Seq(
  "com.lihaoyi"                   %% "fastparse"                 % "2.1.0",

  "org.apache.spark" %% "spark-core" % "2.3.2",
  "org.apache.spark" %% "spark-sql" % "2.3.2",
  "org.apache.hadoop" % "hadoop-common" % "2.7.7"
)