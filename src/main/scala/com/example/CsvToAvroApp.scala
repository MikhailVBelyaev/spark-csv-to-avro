package com.example

import org.apache.spark.sql.{SparkSession, DataFrame}
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types._
import com.typesafe.config.{Config, ConfigFactory}
import scala.util.Try

object CsvToAvroApp {

  def main(args: Array[String]): Unit = {
    val spark = SparkSession.builder()
      .appName("CsvToAvroApp")
      .master(sys.env.getOrElse("SPARK_MASTER_URL", "local[*]"))
      .config("spark.sql.legacy.allowNonEmptyLocationInCTAS", "true")
      .config("spark.metrics.conf.*.sink.console.class", "org.apache.spark.metrics.sink.ConsoleSink")
      .config("spark.metrics.conf.driver.sink.console.period", "1") // Disable metrics
      .config("spark.metrics.conf.executor.sink.console.period", "1") // Disable metrics
      .getOrCreate()

    val conf = ConfigFactory.load().getConfig("app")

    val inputDir = s"/app/${conf.getString("sourceDir")}"
    val outputDir = s"/app/${conf.getString("destDir")}"
    val delimiter = Try(conf.getString("delimiter")).getOrElse(",")
    val dedupKey = conf.getString("dedupKey")
    val partitionCol = Try(conf.getString("partitionColumn")).getOrElse("processing_date")

    val df = spark.read
      .format("csv") // Explicitly specify CSV format
      .option("header", "true")
      .option("delimiter", delimiter)
      .option("inferSchema", "true")
      .load(inputDir)

    val cleaned = process(df, conf.getConfig("schemaMapping"), dedupKey, partitionCol)
    cleaned.write
      .format("avro")
      .mode("overwrite")
      .partitionBy(partitionCol)
      .save(outputDir)

    println(s"✅ Process completed. Output written to $outputDir")
    spark.stop()
  }

  def process(df: DataFrame, schemaConfig: Config, dedupKey: String, partitionCol: String): DataFrame = {
    val casted = safeCastColumns(df, schemaConfig)._1
    val withTimestamp = casted.withColumn(partitionCol, current_date())
    val deduped = withTimestamp.dropDuplicates(dedupKey :: Nil)
    deduped
  }

  def safeCastColumns(df: DataFrame, config: Config): (DataFrame, Long) = {
    var errorCount = 0L
    var result = df
    import df.sparkSession.implicits._

    config.entrySet().forEach { entry =>
      val colName = entry.getKey
      val targetType = config.getString(colName)
      val castExpr = targetType.split(":")(0)
      val fmt = if (targetType.contains(":")) targetType.split(":")(1) else ""

      result = castExpr match {
        case "IntegerType" => result.withColumn(colName, $"${colName}".cast(IntegerType))
        case "DoubleType" => result.withColumn(colName, $"${colName}".cast(DoubleType))
        case "StringType" => result.withColumn(colName, $"${colName}".cast(StringType))
        case "DateType" => result.withColumn(colName, to_date($"${colName}", fmt))
        case _ => result
      }
    }
    (result, errorCount)
  }
}