package com.example

import org.apache.spark.sql.{SparkSession, DataFrame, Row}
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types._
import com.typesafe.config.{Config, ConfigFactory}
import org.apache.logging.log4j.{LogManager, Logger}
import scopt.OParser
import scala.collection.JavaConverters._

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
        case "StringType"    => StringType
        case "IntegerType"   => IntegerType
        case "LongType"      => LongType
        case "DoubleType"    => DoubleType
        case "FloatType"     => FloatType
        case "BooleanType"   => BooleanType
        case "DateType"      => DateType
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
    val orderedCols = conf.getStringList("columns").asScala.toSeq
    val schema = buildSchema(conf.getConfig("schemaMapping"), orderedCols)

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
          .config("spark.sql.session.timeZone", "UTC")
          .config("spark.sql.legacy.allowNonEmptyLocationInCTAS", "true")
          .config("spark.sql.legacy.timeParserPolicy", "LEGACY")
          .getOrCreate()
        spark.sparkContext.setLogLevel("WARN")

        val inputDir = s"/app/${cliConfig.sourceDir}"
        val outputDir = s"/app/${cliConfig.destDir}"
        val delimiter = cliConfig.delimiter
        val dedupKey = cliConfig.dedupKey
        val partitionCol = cliConfig.partitionCol

        // --- READ CSV: ALL STRING, NO HEADER ---
        val df_raw_strings = spark.read
          .format("csv")
          .option("header", "false")
          .option("delimiter", delimiter)
          .option("mode", "PERMISSIVE")
          .option("columnNameOfCorruptRecord", "_corrupt_record")
          .option("inferSchema", "false")
          .load(inputDir)

        val df_named = df_raw_strings.toDF(orderedCols: _*)

        // --- STRUCTURAL CORRUPTIONS (malformed rows) ---
        val hasCorrupt = df_named.columns.contains("_corrupt_record")
        val (df_after_corrupt, df_structural_bad) = if (hasCorrupt) {
          val bad = df_named.filter(col("_corrupt_record").isNotNull)
          val good = df_named.filter(col("_corrupt_record").isNull).drop("_corrupt_record")
          (good, bad)
        } else {
          (df_named, spark.emptyDataFrame)
        }

        if (!df_structural_bad.isEmpty) {
          val path = s"$outputDir/../corrupted/structural_${System.currentTimeMillis()}"
          logger.warn(s"Saving ${df_structural_bad.count()} structural corrupt rows → $path")
          df_structural_bad.write.mode("overwrite").json(path)
        }

        // --- MISSING COLUMNS VALIDATION ---
        import org.apache.spark.sql.functions.{when}

        val expectedCount = orderedCols.size
        val nonNullCountExpr = orderedCols.map { c =>
          when(col(c).isNotNull, 1).otherwise(0)
        }.reduce(_ + _)

        val df_valid_structure = df_after_corrupt.filter(nonNullCountExpr === expectedCount)
        val df_missing_cols = df_after_corrupt.filter(nonNullCountExpr =!= expectedCount)

        if (!df_missing_cols.isEmpty) {
          val path = s"$outputDir/../corrupted/missing_cols_${System.currentTimeMillis()}"
          logger.warn(s"Saving ${df_missing_cols.count()} rows with missing columns → $path")
          df_missing_cols.write.mode("overwrite").json(path)
        }

        val stringSchema = df_valid_structure.schema

        // --- CASTING + CAPTURE TYPE ERRORS ---
        val (df_typed_clean, castErrorCount, df_cast_bad) = safeCastColumns(
          df_valid_structure, conf.getConfig("schemaMapping"), globalDateFmt, globalTsFmt, spark, stringSchema, orderedCols)

        if (!df_cast_bad.isEmpty) {
          val path = s"$outputDir/../corrupted/cast_errors_${System.currentTimeMillis()}"
          logger.warn(s"Saving $castErrorCount casting errors → $path")
          df_cast_bad.write.mode("overwrite").json(path)
        }

        // --- FINAL VALIDATION & WRITE ---
        val validated = df_typed_clean.filter(col(dedupKey).isNotNull)
        val nullKeyCnt = df_typed_clean.count() - validated.count()
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

  // ==================================================================
  // SAFE CASTING: Returns (clean DF, error count, corrupted DF)
  // ==================================================================
  def safeCastColumns(
    df: DataFrame,
    config: Config,
    globalDateFmt: String,
    globalTsFmt: String,
    spark: SparkSession,
    stringSchema: StructType,
    originalCols: Seq[String]
  ): (DataFrame, Long, DataFrame) = {

    import df.sparkSession.implicits._
    import org.apache.spark.sql.functions._

    // 1. Add *_orig for all columns
    val origSelect = originalCols.map(c => col(c).as(s"${c}_orig"))
    var result = df.select(df.columns.map(col) ++ origSelect: _*)

    var totalErrors = 0L
    val badRowBuffer = scala.collection.mutable.ArrayBuffer[Row]()

    // 2. Cast column-by-column
    config.entrySet().forEach { entry =>
      val colName = entry.getKey
      if (result.columns.contains(colName)) {
        val target = config.getString(colName)
        val Array(castExpr, fmtRaw @ _*) = target.split(":", 2)
        val fmt = if (fmtRaw.nonEmpty) fmtRaw(0) else castExpr match {
          case "DateType"      => globalDateFmt
          case "TimestampType" => globalTsFmt
          case _               => ""
        }

        val dataType = castExpr match {
          case "StringType"    => StringType
          case "IntegerType"   => IntegerType
          case "LongType"      => LongType
          case "DoubleType"    => DoubleType
          case "FloatType"     => FloatType
          case "BooleanType"   => BooleanType
          case "DateType"      => DateType
          case "TimestampType" => TimestampType
          case "DecimalType" =>
            val Array(p, s) = fmt.split(",").map(_.trim.toInt)
            DecimalType(p, s)
          case _ => StringType
        }

        val casted = castExpr match {
          case "DateType" if fmt.nonEmpty      => to_date(col(colName), fmt)
          case "TimestampType" if fmt.nonEmpty => to_timestamp(col(colName), fmt)
          case _                               => col(colName).cast(dataType)
        }

        result = result.withColumn(colName, casted)

        // Failed: casted to null but original string was non-empty
        val failed = result.filter(
          casted.isNull &&
          col(s"${colName}_orig").isNotNull &&
          trim(col(s"${colName}_orig")) =!= ""
        )

        val cnt = failed.count()
        totalErrors += cnt
        if (cnt > 0) {
          logger.warn(s"Casting failures for $colName ($castExpr): $cnt")
          failed.take(5).foreach(r => logger.warn(s"Failed row: ${r.mkString(", ")}"))
        }

        val origSelectBad = originalCols.map(c => col(s"${c}_orig").as(c))
        failed.select(origSelectBad: _*).take(100).foreach(badRowBuffer += _)
      }
    }

    // 3. Build corrupted DF from original strings
    val badDf = if (badRowBuffer.nonEmpty) {
      import scala.collection.JavaConverters._
      spark.createDataFrame(badRowBuffer.toSeq.asJava, stringSchema)
    } else {
      spark.emptyDataFrame
    }

    // 4. FINAL FILTER: keep only rows with NO failed casts
    val anyFailedCast = originalCols.map { c =>
      col(c).isNull && col(s"${c}_orig").isNotNull && trim(col(s"${c}_orig")) =!= ""
    }.reduce(_ || _)

    val withFlag = result.withColumn("_any_cast_fail", anyFailedCast)
    val dfCleanWithOrig = withFlag.filter(!col("_any_cast_fail"))

    // 5. Drop _orig only from clean data
    val cleanCols = originalCols.map(col)
    val dfCleanFinal = dfCleanWithOrig.select(cleanCols: _*)

    (dfCleanFinal, totalErrors, badDf)
  }
}