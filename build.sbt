name := "spark-csv-to-avro"
version := "0.1"
scalaVersion := "2.12.18"

libraryDependencies ++= Seq(
  "org.apache.spark" %% "spark-core" % "3.5.0" % "provided",
  "org.apache.spark" %% "spark-sql" % "3.5.0" % "provided",
  "org.apache.spark" %% "spark-avro" % "3.5.0" % "provided",
  "org.apache.spark" %% "spark-hive" % "3.5.0" % "provided",
  "org.apache.spark" %% "spark-catalyst" % "3.5.0" % "provided",
  "com.typesafe" % "config" % "1.4.2",
  "org.apache.logging.log4j" % "log4j-slf4j2-impl" % "2.20.0",
  "org.apache.logging.log4j" % "log4j-core" % "2.20.0",
  "org.apache.logging.log4j" % "log4j-api" % "2.20.0",
  "org.scalatest" %% "scalatest" % "3.2.18" % Test
)

assembly / mainClass := Some("com.example.CsvToAvroApp")
assembly / test := {}

assembly / assemblyMergeStrategy := {
  case PathList("META-INF", xs @ _*) => MergeStrategy.discard
  case PathList("org", "slf4j", xs @ _*) => MergeStrategy.first
  case PathList("org", "apache", "logging", xs @ _*) => MergeStrategy.first
  case "org/slf4j/impl/StaticLoggerBinder.class" => MergeStrategy.first
  case x => MergeStrategy.deduplicate
}

assembly / assemblyOption := (assembly / assemblyOption).value.copy(
  includeScala = true,
  includeDependency = true
)

dependencyOverrides ++= Seq(
  "org.slf4j" % "slf4j-api" % "2.0.7",
  "org.apache.logging.log4j" % "log4j-slf4j2-impl" % "2.20.0"
)

// Copy the assembled JAR to jar_output/
assembly := {
  val jar = assembly.value
  val targetDir = file("jar_output")
  IO.copyFile(jar, targetDir / jar.getName)
  jar
}