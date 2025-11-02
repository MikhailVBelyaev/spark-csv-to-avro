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

  def buildSchema(schemaConfig: Config, ordered: Seq[String]): StructType = {
    val fields = ordered.map { name =>
      val raw = schemaConfig.getString(name)
      val parts = raw.split(":")
      val typeName = parts(0)
      val fmt = if (parts.length > 1) parts(1) else ""

      val dataType = typeName match {
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
        case _ => StringType
      }

      StructField(name, dataType, nullable = true)
    }
    StructType(fields)
  }

  def main(args: Array[String]): Unit = {
    val conf = ConfigFactory.load().getConfig("app")
    val globalDateFmt = conf.getString("dateFormat")
    val globalTsFmt = conf.getString("timestampFormat")
    import scala.collection.JavaConverters._
    val orderedCols = conf.getStringList("columns").asScala
    val schema = buildSchema(conf.getConfig("schemaMapping"), orderedCols)

    // Command-line arg parsing
    val builder = OParser.builder[AppConfig]
    val parser = {
      import builder._
      OParser.sequence(
        programName("CsvToAvroApp"),
        head("CsvToAvroApp", "0.1"),
        opt[String]('s', "sourceDir").action((x, c) => c.copy(sourceDir = x)).text("Source directory"),
        opt[String]('d', "destDir").action((x, c) => c.copy(destDir = x)).text("Destination directory"),
        opt[String]('l', "delimiter").action((x, c) => c.copy(delimiter = x)).text("Delimiter"),
        opt[String]('k', "dedupKey").action((x, c) => c.copy(dedupKey = x)).text("Deduplication key"),
        opt[String]('p', "partitionCol").action((x, c) => c.copy(partitionCol = x)).text("Partition column")
      )
    }

    OParser.parse(parser, args, AppConfig()) match {
      case Some(cliConfig) =>
        val spark = SparkSession.builder()
          .appName("CsvToAvroApp")
          .master(sys.env.getOrElse("SPARK_MASTER_URL", "local[*]"))
          .config("spark.sql.session.timeZone", "UTC+5")
          .config("spark.sql.legacy.allowNonEmptyLocationInCTAS", "true")
          .config("spark.sql.legacy.timeParserPolicy", "LEGACY")
          .getOrCreate()

        val inputDir = s"/app/${cliConfig.sourceDir}"
        val outputDir = s"/app/${cliConfig.destDir}"
        val delimiter = cliConfig.delimiter
        val dedupKey = cliConfig.dedupKey
        val partitionCol = cliConfig.partitionCol

        // --- READ CSV: NO SCHEMA, ALL STRING, NO HEADER ---
        val df_raw_strings = spark.read
          .format("csv")
          .option("header", "false")
          .option("delimiter", delimiter)
          .option("mode", "PERMISSIVE")
          .option("columnNameOfCorruptRecord", "_corrupt_record")
          .option("inferSchema", "false")  // ← ADDED
          .load(inputDir)

        // Rename _c0, _c1, ... → id, name, ...
        val df_named = df_raw_strings.toDF(orderedCols: _*)

        // --- STRUCTURAL CORRUPTIONS ---
        val hasCorrupt = df_named.columns.contains("_corrupt_record")
        val (df_clean_strings, df_structural_bad) = if (hasCorrupt) {
          val bad  = df_named.filter(col("_corrupt_record").isNotNull)
          val good = df_named.filter(col("_corrupt_record").isNull).drop("_corrupt_record")
          (good, bad)
        } else {
          (df_named, spark.emptyDataFrame)
        }

        // Save structural bad rows
        if (!df_structural_bad.isEmpty) {
          val path = s"$outputDir/../corrupted/structural_${System.currentTimeMillis()}"
          logger.warn(s"Saving ${df_structural_bad.count()} structural corrupt rows → $path")
          df_structural_bad.write.mode("overwrite").json(path)
        }

        // --- CASTING + CAPTURE TYPE ERRORS ---
        val (df_typed, castErrorCount, df_cast_bad) = safeCastColumns(
          df_clean_strings, conf.getConfig("schemaMapping"), globalDateFmt, globalTsFmt)

        // Save casting errors
        if (!df_cast_bad.isEmpty) {
          val path = s"$outputDir/../corrupted/cast_errors_${System.currentTimeMillis()}"
          logger.warn(s"Saving $castErrorCount casting errors → $path")
          df_cast_bad.write.mode("overwrite").json(path)
        }

        // --- VALIDATION, DEDUP, WRITE ---
        val validated = df_typed.filter(col(dedupKey).isNotNull)
        val nullKeyCnt = df_typed.count() - validated.count()
        if (nullKeyCnt > 0) logger.warn(s"Filtered $nullKeyCnt rows with null $dedupKey")

        val withTs = validated.withColumn(partitionCol, current_timestamp())
        val finalDf = withTs.dropDuplicates(dedupKey :: Nil)

        finalDf.write
          .format("avro")
          .mode("overwrite")
          .partitionBy(partitionCol)
          .save(outputDir)

        val written = spark.read.format("avro").load(outputDir).count()
        logger.info(s"Records written (clean): $written")
        logger.info(s"Process completed. Output written to $outputDir")

        if (!sys.props.contains("spark.test.active")) spark.stop()

      case None =>
        OParser.usage(parser)
        sys.exit(1)
    }
  }

  // --- UPDATED: returns bad rows too ---
  def safeCastColumns(
      df: DataFrame,
      config: Config,
      globalDateFmt: String,
      globalTsFmt: String
  ): (DataFrame, Long, DataFrame) = {

    import df.sparkSession.implicits._
    var result = df
    var errorCount = 0L
    val badRows = scala.collection.mutable.ArrayBuffer[Row]()

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

        val origCol = s"${colName}_orig"
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
          case _ => StringType
        }

        if (castExpr == "DateType" && fmt.nonEmpty) {
          result = result.withColumn(colName, to_date(col(colName), fmt))
        } else if (castExpr == "TimestampType" && fmt.nonEmpty) {
          result = result.withColumn(colName, to_timestamp(col(colName), fmt))
        } else {
          result = result.withColumn(colName, col(colName).cast(castType))
        }

        val failures = result.filter(col(colName).isNull && col(origCol).isNotNull)
        val failCount = failures.count()
        errorCount += failCount
        if (failCount > 0) {
          logger.warn(s"Casting failures for $colName ($castExpr): $failCount")
          failures.take(5).foreach { row =>
            logger.warn(s"Failed row: ${row.mkString(", ")}")
          }
          failures.take(100).foreach(badRows += _)  // collect for saving
        }
        result = result.drop(origCol)
      }
    }

    val badDf = if (badRows.nonEmpty)
      spark.createDataFrame(badRows.toSeq, result.schema)
    else
      spark.emptyDataFrame

    (result, errorCount, badDf)
  }
}