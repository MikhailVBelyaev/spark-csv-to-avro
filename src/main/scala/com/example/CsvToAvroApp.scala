package com.example

import org.apache.spark.sql.{SparkSession, DataFrame}
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
  // MAIN
  // ==========================================================
  def main(args: Array[String]): Unit = {

    val conf = ConfigFactory.load().getConfig("app")
    val orderedCols = conf.getStringList("columns").asScala.toSeq
    val schemaCfg   = conf.getConfig("schemaMapping")

    val dateFmt = conf.getString("dateFormat")
    val tsFmt   = conf.getString("timestampFormat")

    val spark = SparkSession.builder()
      .appName("CsvToAvroApp")
      .master(sys.env.getOrElse("SPARK_MASTER_URL", "local[*]"))
      .config("spark.sql.session.timeZone", "UTC")
      .getOrCreate()

    spark.sparkContext.setLogLevel("WARN")

    val inputDir  = "/app/data/input"
    val outputDir = "/app/data/output"

    // ==========================================================
    // 1. READ RAW CSV (ALL STRING)
    // ==========================================================
    val rawDf = spark.read
      .format("csv")
      .option("header", "false")
      .option("delimiter", ",")
      .option("mode", "PERMISSIVE")
      .load(inputDir)

    val expectedCols = orderedCols.size

    // ==========================================================
    // 2. STRUCTURAL VALIDATION
    // ==========================================================
    val withCount =
      rawDf.withColumn("_col_count", size(array(rawDf.columns.map(col): _*)))

    val structuralBad =
      withCount.filter(col("_col_count") =!= expectedCols)
        .drop("_col_count")

    val structuralOk =
      withCount.filter(col("_col_count") === expectedCols)
        .drop("_col_count")
        .toDF(orderedCols: _*)

    if (!structuralBad.isEmpty) {
      structuralBad
        .withColumn("error_reason", lit("COLUMN_COUNT_MISMATCH"))
        .write.mode("overwrite")
        .json(s"$outputDir/../corrupted/structural")
    }

    // ==========================================================
    // 3. REQUIRED COLUMNS VALIDATION (CRITICAL FIX)
    // ==========================================================
    val requiredExpr =
      orderedCols.map(c => col(c).isNotNull && trim(col(c)) =!= "").reduce(_ && _)

    val missingRequired =
      structuralOk.filter(!requiredExpr)

    val requiredOk =
      structuralOk.filter(requiredExpr)

    if (!missingRequired.isEmpty) {
      missingRequired
        .withColumn("error_reason", lit("MISSING_REQUIRED_COLUMNS"))
        .write.mode("overwrite")
        .json(s"$outputDir/../corrupted/missing")
    }

    // ==========================================================
    // 4. SAFE CAST (ROW-LEVEL)
    // ==========================================================
    val casted =
      schemaCfg.entrySet().asScala.foldLeft(requiredOk) { (df, e) =>
        val colName = e.getKey
        val tpe = e.getValue.unwrapped().toString.split(":")(0)

        val castCol = tpe match {
          case "IntegerType"   => col(colName).cast(IntegerType)
          case "LongType"      => col(colName).cast(LongType)
          case "DoubleType"    => col(colName).cast(DoubleType)
          case "FloatType"     => col(colName).cast(FloatType)
          case "BooleanType"   => col(colName).cast(BooleanType)
          case "DateType"      => to_date(col(colName), dateFmt)
          case "TimestampType" => to_timestamp(col(colName), tsFmt)
          case _               => col(colName)
        }

        df.withColumn(colName, castCol)
          .withColumn(s"${colName}_bad",
            when(
              castCol.isNull &&
              col(colName).isNotNull &&
              trim(col(colName)) =!= "",
              lit(true)
            ).otherwise(lit(false))
          )
      }

    val anyCastFail =
      orderedCols.map(c => col(s"${c}_bad")).reduce(_ || _)

    val castBad =
      casted.filter(anyCastFail)
        .select(orderedCols.map(col): _*)

    val clean =
      casted.filter(!anyCastFail)
        .select(orderedCols.map(col): _*)

    if (!castBad.isEmpty) {
      castBad
        .withColumn("error_reason", lit("TYPE_CAST_FAILED"))
        .write.mode("overwrite")
        .json(s"$outputDir/../corrupted/cast")
    }

    // ==========================================================
    // 5. FINAL WRITE
    // ==========================================================
    val finalDf =
      clean
        .filter(col("id").isNotNull)
        .withColumn("processing_timestamp", current_timestamp())
        .dropDuplicates("id")

    finalDf.write
      .format("avro")
      .mode("overwrite")
      .partitionBy("processing_timestamp")
      .save(outputDir)

    logger.warn(s"VALID ROWS: ${finalDf.count()}")
    spark.stop()
  }
}