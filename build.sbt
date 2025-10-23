name := "spark-csv-to-avro"
version := "0.1"
scalaVersion := "2.12.18"

libraryDependencies ++= Seq(
  "org.apache.spark" %% "spark-core" % "3.5.0",
  "org.apache.spark" %% "spark-sql" % "3.5.0",
  "org.apache.spark" %% "spark-avro" % "3.5.0",
  "org.apache.spark" %% "spark-hive" % "3.5.0",
  "org.apache.spark" %% "spark-catalyst" % "3.5.0",
  "com.typesafe" % "config" % "1.4.2",
  // Logging dependencies
  "org.apache.logging.log4j" % "log4j-slf4j2-impl" % "2.20.0",
  "org.apache.logging.log4j" % "log4j-core" % "2.20.0",
  "org.apache.logging.log4j" % "log4j-api" % "2.20.0",
  "org.scalatest" %% "scalatest" % "3.2.18" % Test
)

assembly / mainClass := Some("com.example.CsvToAvroApp")
assembly / test := {}

assembly / assemblyMergeStrategy := {
  case PathList("META-INF", xs @ _*) => MergeStrategy.discard
  case PathList("org", "apache", "spark", xs @ _*) => MergeStrategy.last // Ensure spark-sql classes
  case PathList("org", "slf4j", xs @ _*) => MergeStrategy.last // Prioritize log4j-slf4j2-impl
  case PathList("org", "apache", "logging", xs @ _*) => MergeStrategy.last // Ensure log4j-core and log4j-api
  case "org/slf4j/impl/StaticLoggerBinder.class" => MergeStrategy.last // Prioritize SLF4J binding
  case x => MergeStrategy.first
}

assembly / assemblyOption := (assembly / assemblyOption).value.copy(
  includeScala = true,
  includeDependency = true
)

// Explicitly override conflicting SLF4J bindings
dependencyOverrides ++= Seq(
  "org.slf4j" % "slf4j-api" % "2.0.7",
  "org.apache.logging.log4j" % "log4j-slf4j2-impl" % "2.20.0"
)

// Exclude conflicting transitive dependencies
libraryDependencies ++= Seq(
  "org.apache.spark" %% "spark-core" % "3.5.0" exclude("org.slf4j", "slf4j-simple") exclude("ch.qos.logback", "logback-classic") exclude("org.slf4j", "slf4j-log4j12"),
  "org.apache.spark" %% "spark-sql" % "3.5.0" exclude("org.slf4j", "slf4j-simple") exclude("ch.qos.logback", "logback-classic") exclude("org.slf4j", "slf4j-log4j12")
)