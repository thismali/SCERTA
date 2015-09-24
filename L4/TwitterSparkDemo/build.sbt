name := "labs"

version := "1.0"

scalaVersion := "2.11.7"

libraryDependencies ++= Seq(

  "org.apache.spark" %% "spark-core" % "1.4.0" % "provided",
  "org.apache.spark" %% "spark-streaming" % "1.4.0",
  "org.apache.spark" %% "spark-streaming-twitter" % "1.4.0",
  "org.apache.spark" %% "spark-mllib" % "1.4.0",
  "org.apache.commons" % "commons-lang3" % "3.0",
  "org.eclipse.jetty" % "jetty-client" % "8.1.14.v20131031",
  "com.typesafe.play" % "play-json_2.10" % "2.4.2",
  //"org.elasticsearch" % "elasticsearch-hadoop-mr" % "2.0.0.RC1",
  "net.sf.opencsv" % "opencsv" % "2.0",
  "com.github.scopt" %% "scopt" % "3.2.0",
  "org.scalatest" %% "scalatest" % "2.2.1" % "test",
  "org.scalaj" %% "scalaj-http" % "1.1.5",
  "com.github.fommil.netlib" % "all" % "1.1",
  //"edu.stanford.nlp" % "stanford-corenlp" % "3.3.0",
  //"edu.stanford.nlp" % "stanford-corenlp" % "3.3.0" classifier "models",
  //"com.amazonaws" % "amazon-kinesis-client" % "1.4.0",
  "org.apache.spark" %% "spark-streaming-kinesis-asl" % "1.4.0"
  //"com.amazonaws" % "aws-java-sdk" % "1.10.2"
)
dependencyOverrides ++= Set(
  "com.fasterxml.jackson.core" % "jackson-databind" % "2.4.4"
)
