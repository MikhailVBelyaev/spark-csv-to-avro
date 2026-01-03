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

  // ==========================================================
  // BUILD TARGET SCHEMA (VALID DATA ONLY)
  // ==========================================================
  def buildSchema(schemaConfig: Config, ordered: Seq[String]): StructType =
    StructType(
      ordered.map { name =>
        val raw = schemaConfig.getString(name)
        val parts = raw.split(":")
        val tpe = parts(0)
        val fmt = if (parts.length > 1) parts(1) else ""

        val dt = tpe match {
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

        StructField(name, dt, nullable = true)
      }
    )

  // ==========================================================
  // MAIN
  // ==========================================================
  def main(args: Array[String]): Unit = {

    val conf = ConfigFactory.load().getConfig("app")
    val orderedCols = conf.getStringList("columns").asScala.toSeq

    val globalDateFmt = conf.getString("dateFormat")
    val globalTsFmt   = conf.getString("timestampFormat")

    val builder = OParser.builder[AppConfig]
    val parser = {
      import builder._
      OParser.sequence(
        programName("CsvToAvroApp"),
        opt[String]("sourceDir").action((x, c) => c.copy(sourceDir = x)),
        opt[String]("destDir").action((x, c) => c.copy(destDir = x)),
        opt[String]("delimiter").action((x, c) => c.copy(delimiter = x)),
        opt[String]("dedupKey").action((x, c) => c.copy(dedupKey = x)),
        opt[String]("partitionCol").action((x, c) => c.copy(partitionCol = x))
      )
    }

    OParser.parse(parser, args, AppConfig()) match {
      case Some(cli) =>

        val spark = SparkSession.builder()
          .appName("CsvToAvroApp")
          .master(sys.env.getOrElse("SPARK_MASTER_URL", "local[*]"))
          .config("spark.sql.session.timeZone", "UTC")
          .config("spark.sql.legacy.timeParserPolicy", "LEGACY")
          .getOrCreate()

        spark.sparkContext.setLogLevel("WARN")

        val inputDir  = s"/app/${cli.sourceDir}"
        val outputDir = s"/app/${cli.destDir}"

        // ==========================================================
        // 1. READ RAW CSV (STRING ONLY)
        // ==========================================================
        val rawDf = spark.read
          .format("csv")
          .option("header", "false")
          .option("delimiter", cli.delimiter)
          .option("mode", "PERMISSIVE")
          .option("inferSchema", "false")
          .load(inputDir)

        val expectedCols = orderedCols.size

        // ==========================================================
        // 2. STRUCTURAL VALIDATION (EVE FIX)
        // ==========================================================
        val withColCount =
          rawDf.withColumn("_col_count", size(array(rawDf.columns.map(col): _*)))

        val df_structural_bad =
          withColCount.filter(col("_col_count") =!= expectedCols)
            .drop("_col_count")

        val df_structural_ok =
          withColCount.filter(col("_col_count") === expectedCols)
            .drop("_col_count")

        if (!df_structural_bad.isEmpty) {
          val path = s"$outputDir/../corrupted/structural_${System.currentTimeMillis()}"
          logger.warn(s"Structural errors: ${df_structural_bad.count()}")
          df_structural_bad
            .withColumn("error_reason", lit("COLUMN_COUNT_MISMATCH"))
            .write.mode("overwrite").json(path)
        }

        // ==========================================================
        // 3. SAFE COLUMN NAMING
        // ==========================================================
        val df_named = df_structural_ok.toDF(orderedCols: _*)

        // ==========================================================
        // 4. CAST + TYPE VALIDATION (SAFE)
        // ==========================================================
        val (df_clean, _, df_cast_bad) =
          safeCastColumns(
            df_named,
            conf.getConfig("schemaMapping"),
            globalDateFmt,
            globalTsFmt,
            spark,
            orderedCols
          )

        if (!df_cast_bad.isEmpty) {
          val path = s"$outputDir/../corrupted/cast_${System.currentTimeMillis()}"
          df_cast_bad.write.mode("overwrite").json(path)
        }

        // ==========================================================
        // 5. FINAL CLEAN WRITE
        // ==========================================================
        val finalDf =
          df_clean
            .filter(col(cli.dedupKey).isNotNull)
            .withColumn(cli.partitionCol, current_timestamp())
            .dropDuplicates(cli.dedupKey :: Nil)

        finalDf.write
          .format("avro")
          .mode("overwrite")
          .partitionBy(cli.partitionCol)
          .save(outputDir)

        logger.info(s"Clean rows written: ${finalDf.count()}")
        spark.stop()

      case None => sys.exit(1)
    }
  }

  // ==========================================================
  // SAFE CASTING (NO CRASH VERSION)
  // ==========================================================
  def safeCastColumns(
    df: DataFrame,
    config: Config,
    globalDateFmt: String,
    globalTsFmt: String,
    spark: SparkSession,
    orderedCols: Seq[String]
  ): (DataFrame, Long, DataFrame) = {

    import spark.implicits._

    // STRING schema for corrupted rows (CRITICAL)
    val stringSchema =
      StructType(orderedCols.map(c => StructField(c, StringType, true)))

    var result =
      df.select(df.columns.map(col) ++ orderedCols.map(c => col(c).as(s"${c}_orig")): _*)

    val badRows = scala.collection.mutable.ArrayBuffer[Row]()
    var errorCount = 0L

    config.entrySet().forEach { e =>
      val colName = e.getKey
      val castType = e.getValue.unwrapped().toString.split(":")(0)

      val casted = castType match {
        case "IntegerType"   => col(colName).cast(IntegerType)
        case "LongType"      => col(colName).cast(LongType)
        case "DoubleType"    => col(colName).cast(DoubleType)
        case "FloatType"     => col(colName).cast(FloatType)
        case "BooleanType"   => col(colName).cast(BooleanType)
        case "DateType"      => to_date(col(colName), globalDateFmt)
        case "TimestampType" => to_timestamp(col(colName), globalTsFmt)
        case _               => col(colName)
      }

      result = result.withColumn(colName, casted)

      val failed =
        result.filter(
          casted.isNull &&
          col(s"${colName}_orig").isNotNull &&
          trim(col(s"${colName}_orig")) =!= ""
        )

      errorCount += failed.count()

      failed
        .select(orderedCols.map(c => col(s"${c}_orig").as(c)): _*)
        .collect()
        .foreach(badRows += _)
    }

    val badDf =
      if (badRows.nonEmpty)
        spark.createDataFrame(badRows.toSeq.asJava, stringSchema)
      else
        spark.emptyDataFrame

    val anyFail =
      orderedCols.map(c =>
        col(c).isNull && col(s"${c}_orig").isNotNull && trim(col(s"${c}_orig")) =!= ""
      ).reduce(_ || _)

    val cleanDf =
      result.filter(!anyFail).select(orderedCols.map(col): _*)

    (cleanDf, errorCount, badDf)
  }
}