name := "spark-csv-to-avro"
version := "0.1"
scalaVersion := "2.12.18"

libraryDependencies ++= Seq(
  "org.apache.spark" %% "spark-core" % "3.5.0",
  "org.apache.spark" %% "spark-sql" % "3.5.0",
  "org.apache.spark" %% "spark-avro" % "3.5.0",
  "org.apache.spark" %% "spark-hive" % "3.5.0",
  "com.typesafe" % "config" % "1.4.2",
  "ch.qos.logback" % "logback-classic" % "1.4.14",
  "org.scalatest" %% "scalatest" % "3.2.18" % Test
)

assembly / mainClass := Some("com.example.CsvToAvroApp")
assembly / test := {}

assembly / assemblyMergeStrategy := {
  case PathList("META-INF", xs @ _*) => MergeStrategy.discard
  case x => MergeStrategy.first
}

assembly / assemblyOption := (assembly / assemblyOption).value.copy(
  includeScala = true,
  includeDependency = true
)