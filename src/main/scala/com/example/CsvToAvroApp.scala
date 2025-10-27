package com.example

import org.apache.spark.sql.{SparkSession, DataFrame, Row}
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types._
import com.typesafe.config.{Config, ConfigFactory}
import scala.util.Try
import org.apache.logging.log4j.{LogManager, Logger}
import scopt.OParser

object CsvToAvroApp {

  private val logger: Logger = LogManager.getLogger(getClass)

  case class AppConfig(
    sourceDir: String = "data/input",
    destDir: String = "data/output",
    delimiter: String = ",",
    dedupKey: String = "id",
    partitionCol: String = "processing_timestamp"
  )

  def main(args: Array[String]): Unit = {
    val conf = ConfigFactory.load().getConfig("app")
    val globalDateFmt = conf.getString("dateFormat")
    val globalTsFmt = conf.getString("timestampFormat")

    // Command-line arg parsing with scopt 4.1.0
    val builder = OParser.builder[AppConfig]
    val parser = {
      import builder._
      OParser.sequence(
        programName("CsvToAvroApp"),
        head("CsvToAvroApp", "0.1"),
        opt[String]('s', "sourceDir")
          .action((x, c) => c.copy(sourceDir = x))
          .text("Source directory"),
        opt[String]('d', "destDir")
          .action((x, c) => c.copy(destDir = x))
          .text("Destination directory"),
        opt[String]('l', "delimiter")
          .action((x, c) => c.copy(delimiter = x))
          .text("Delimiter"),
        opt[String]('k', "dedupKey")
          .action((x, c) => c.copy(dedupKey = x))
          .text("Deduplication key"),
        opt[String]('p', "partitionCol")
          .action((x, c) => c.copy(partitionCol = x))
          .text("Partition column")
      )
    }

    OParser.parse(parser, args, AppConfig()) match {
      case Some(cliConfig) =>
        val spark = SparkSession.builder()
          .appName("CsvToAvroApp")
          .master(sys.env.getOrElse("SPARK_MASTER_URL", "local[*]"))
          .config("spark.sql.legacy.allowNonEmptyLocationInCTAS", "true")
          .getOrCreate()

        // Override conf with CLI args if provided
        val inputDir = s"/app/${cliConfig.sourceDir}"
        val outputDir = s"/app/${cliConfig.destDir}"
        val delimiter = cliConfig.delimiter
        val dedupKey = cliConfig.dedupKey
        val partitionCol = cliConfig.partitionCol

        val readStart = spark.read
          .format("csv")
          .option("header", "true")
          .option("delimiter", delimiter)
          .option("inferSchema", "true")
          .option("mode", "DROPMALFORMED")  // Handle malformed gracefully

        val df = readStart.load(inputDir)
        val readCount = df.count()
        logger.info(s"Records read: $readCount")

        val malformedCount = Try(readStart.option("mode", "PERMISSIVE").load(inputDir).count() - readCount).getOrElse(0L)
        if (malformedCount > 0) logger.warn(s"Malformed records dropped: $malformedCount")

        val cleaned = process(df, conf.getConfig("schemaMapping"), dedupKey, partitionCol, globalDateFmt, globalTsFmt)
        cleaned.write
          .format("avro")
          .mode("overwrite")
          .partitionBy(partitionCol)
          .save(outputDir)

        val writtenCount = spark.read.format("avro").load(outputDir).count()
        logger.info(s"Records written: $writtenCount")

        logger.info(s"✅ Process completed. Output written to $outputDir")
        spark.stop()
      case None =>
        // Invalid args, print usage
        OParser.usage(parser)
        sys.exit(1)
    }
  }

  def process(df: DataFrame, schemaConfig: Config, dedupKey: String, partitionCol: String, globalDateFmt: String, globalTsFmt: String): DataFrame = {
    val (casted, errorCount) = safeCastColumns(df, schemaConfig, globalDateFmt, globalTsFmt)
    if (errorCount > 0) logger.warn(s"Total casting errors: $errorCount")

    // Basic validation: e.g., null checks on dedupKey
    val validated = casted.filter(col(dedupKey).isNotNull)
    val invalidCount = casted.count() - validated.count()
    if (invalidCount > 0) logger.warn(s"Invalid records filtered (null in $dedupKey): $invalidCount")

    val withTimestamp = validated.withColumn(partitionCol, current_timestamp())
    val deduped = withTimestamp.dropDuplicates(dedupKey :: Nil)
    deduped
  }

  def safeCastColumns(df: DataFrame, config: Config, globalDateFmt: String, globalTsFmt: String): (DataFrame, Long) = {
    var errorCount = 0L
    var result = df
    import df.sparkSession.implicits._

    config.entrySet().forEach { entry =>
      val colName = entry.getKey
      if (result.columns.contains(colName)) {
        val targetType = config.getString(colName)
        val parts = targetType.split(":")
        val castExpr = parts(0)
        val fmt = if (parts.length > 1) parts(1) else castExpr match {
          case "DateType" => globalDateFmt
          case "TimestampType" => globalTsFmt
          case _ => ""
        }

        // Cache original column for error detection
        val origCol = colName + "_orig"
        result = result.withColumn(origCol, col(colName))

        val castType = castExpr match {
          case "StringType" => StringType
          case "IntegerType" => IntegerType
          case "LongType" => LongType
          case "DoubleType" => DoubleType
          case "FloatType" => FloatType
          case "BooleanType" => BooleanType
          case "DateType" => DateType
          case "TimestampType" => TimestampType
          case "DecimalType" =>
            val Array(prec, scale) = fmt.split(",").map(_.trim.toInt)
            DecimalType(prec, scale)
          case _ => StringType  // Fallback
        }

        // Safe cast: set to null on failure - skip initial cast for Date/Timestamp with custom formats
        if (castExpr == "DateType" && fmt.nonEmpty) {
          result = result.withColumn(colName, to_date(col(colName), fmt))
        } else if (castExpr == "TimestampType" && fmt.nonEmpty) {
          result = result.withColumn(colName, to_timestamp(col(colName), fmt))
        } else {
          result = result.withColumn(colName, col(colName).cast(castType))
        }

        // Detect failures: where cast is null but orig was not
        val failures = result.filter(col(colName).isNull && col(origCol).isNotNull)
        val failCount = failures.count()
        errorCount += failCount
        if (failCount > 0) {
          logger.warn(s"Casting failures for $colName ($castExpr): $failCount")
          // Log sample rows (up to 5)
          failures.take(5).foreach { row: Row =>
            logger.warn(s"Failed row: ${row.mkString(", ")}")
          }
        }

        result = result.drop(origCol)
      } else {
        logger.warn(s"Column $colName not found in DataFrame, skipping casting")
      }
    }
    (result, errorCount)
  }
}