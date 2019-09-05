name := "json-schema-scala"

version := "0.1"

scalaVersion := "2.11.11"

libraryDependencies ++= Seq(
  "com.lihaoyi"                   %% "fastparse"                 % "2.1.0",
  "com.typesafe.scala-logging"    %%  "scala-logging-slf4j"      % "2.1.2",
  "ch.qos.logback"                %   "logback-classic"          % "1.1.7",
  "org.specs2"                    %%  "specs2-core"              % "3.8.4" % "test",
  "org.specs2"                    %%  "specs2-matcher-extra"     % "3.8.4" % "test",
  "org.specs2"                    %%  "specs2-junit"             % "3.8.4" % "test",
  "com.typesafe.play"             %% "play-json"                 % "2.7.0"
)