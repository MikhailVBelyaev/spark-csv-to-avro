package com.example

import org.apache.spark.sql.{SparkSession, DataFrame}
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types._
import com.typesafe.config.{Config, ConfigFactory}
import org.apache.logging.log4j.{LogManager, Logger}
import scala.collection.JavaConverters._

object CsvToAvroApp {
  private val logger: Logger = LogManager.getLogger(getClass)

  def main(args: Array[String]): Unit = {
    val conf = ConfigFactory.load().getConfig("app")
    val orderedCols = conf.getStringList("columns").asScala.toSeq
    val schemaCfg = conf.getConfig("schemaMapping")
    val dateFmt = conf.getString("dateFormat")
    val tsFmt = conf.getString("timestampFormat")

    val spark = SparkSession.builder()
      .appName("CsvToAvroApp")
      .master(sys.env.getOrElse("SPARK_MASTER_URL", "local[*]"))
      .config("spark.sql.session.timeZone", "UTC")
      .getOrCreate()

    import spark.implicits._  // Needed for $ notation and some functions

    spark.sparkContext.setLogLevel("WARN")

    val inputDir = "/app/data/input"
    val outputDir = "/app/data/output"
    val corruptBase = s"$outputDir/../corrupted"

    // 1. Read raw CSV — all columns as string, no header
    val rawDf = spark.read
      .option("header", "false")
      .option("delimiter", ",")
      .option("mode", "PERMISSIVE")
      .csv(inputDir)

    val expectedColCount = orderedCols.size

    // 2. Structural validation: check column count
    val rawWithArray = rawDf.withColumn("_cols_array", array(rawDf.columns.map(c => col(c)): _*))

    val structuralOk = rawWithArray
      .filter(size(col("_cols_array")) === expectedColCount)
      .select(
        (0 until expectedColCount).map(i => col("_cols_array")(i).as(orderedCols(i))): _*
      )

    val structuralBad = rawWithArray
      .filter(size(col("_cols_array")) =!= expectedColCount)
      .drop("_cols_array")

    if (!structuralBad.isEmpty) {
      val count = structuralBad.count()
      logger.warn(s"Structural errors (wrong column count): $count rows")
      structuralBad
        .withColumn("error_reason", lit("WRONG_COLUMN_COUNT"))
        .write.mode("overwrite").json(s"$corruptBase/structural")
    }

    // 3. Start with structurally valid rows (all strings)
    var dfWithCastsAndFlags = structuralOk

    // Keep original string values for error detection
    // We'll add _orig columns first
    val origColsExpr = orderedCols.map(c => col(c).as(s"${c}_orig"))

    dfWithCastsAndFlags = dfWithCastsAndFlags.select(orderedCols.map(col) ++ origColsExpr: _*)

    // 4. Now cast each column and flag failures
    for (colName <- orderedCols) {
      val typeStr = schemaCfg.getString(colName).split(":").head

      val castedCol = typeStr match {
        case "DateType"      => to_date(col(s"${colName}_orig"), dateFmt)
        case "TimestampType" => to_timestamp(col(s"${colName}_orig"), tsFmt)
        case _               => col(s"${colName}_orig").cast(typeStr)
      }

      dfWithCastsAndFlags = dfWithCastsAndFlags
        .withColumn(colName, castedCol)
        .withColumn(
          s"${colName}_cast_bad",
          when(
            col(colName).isNull &&
            trim(col(s"${colName}_orig")) =!= "" &&
            col(s"${colName}_orig").isNotNull,
            lit(true)
          ).otherwise(lit(false))
        )
    }

    // 5. Detect rows with any casting failure
    val badCastFlags = orderedCols.map(c => col(s"${c}_cast_bad"))

    val anyCastFailed = if (badCastFlags.nonEmpty) {
      badCastFlags.reduce(_ || _)
    } else {
      lit(false)
    }

    val castCleanDf = dfWithCastsAndFlags
      .filter(!anyCastFailed)
      .select(orderedCols.map(col): _*)  // Only final casted columns

    val castBadDf = dfWithCastsAndFlags
      .filter(anyCastFailed)
      .select(orderedCols.map(c => col(s"${c}_orig").as(c)): _*)  // Original strings

    if (!castBadDf.isEmpty) {
      val count = castBadDf.count()
      logger.warn(s"Casting errors: $count rows")
      castBadDf
        .withColumn("error_reason", lit("CASTING_FAILED"))
        .write.mode("overwrite").json(s"$corruptBase/cast")
    }

    // 6. Final processing: remove null IDs, deduplicate by id, add timestamp
    val finalDf = castCleanDf
      .filter(col("id").isNotNull)
      .dropDuplicates("id")  // Dedup, keep first
      .withColumn("processing_timestamp", current_timestamp())

    // 7. Write to Avro
    finalDf.write
      .format("avro")
      .mode("overwrite")
      .partitionBy("processing_timestamp")
      .save(outputDir)

    val validCount = finalDf.count()
    logger.warn(s"VALID ROWS: $validCount")

    spark.stop()
  }
}