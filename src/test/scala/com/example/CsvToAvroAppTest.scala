package com.example

import org.scalatest.funsuite.AnyFunSuite
import org.scalatest.BeforeAndAfterAll
import org.apache.spark.sql.{SparkSession, Row}
import org.apache.spark.sql.types._
import org.apache.spark.sql.functions._
import com.typesafe.config.ConfigFactory
import java.nio.file.{Files, Paths}
import java.sql.{Date, Timestamp}
import scala.util.Try
import scala.collection.JavaConverters._

class CsvToAvroAppTest extends AnyFunSuite with BeforeAndAfterAll {

  System.setProperty("spark.test.active", "true")

  val spark: SparkSession = SparkSession.builder()
    .master("local[*]")
    .appName("TestApp")
    .config("spark.sql.session.timeZone", "UTC+5")
    .config("spark.sql.legacy.allowNonEmptyLocationInCTAS", "true")
    .config("spark.sql.legacy.timeParserPolicy", "LEGACY")
    .getOrCreate()

  import spark.implicits._

  override def afterAll(): Unit = spark.stop()

  def withTempDir(testCode: String => Unit): Unit = {
    val tmp = Files.createTempDirectory("spark_test").toString
    try testCode(tmp)
    finally {
      Try {
        Files.walk(Paths.get(tmp))
          .sorted(java.util.Comparator.reverseOrder())
          .forEach(Files.deleteIfExists(_))
      }
    }
  }

  // =========================================================================
  // 1. Type casting – all supported types
  // =========================================================================
  test("type casting – all supported types") {
    val raw = Seq((
      "1", "Alice", "99.99", "25", "1.65", "true",
      "2023-01-01", "2023-01-01 10:30:00", "123.45"
    )).toDF("id","name","price","age","height","is_active",
            "created_date","updated_at","balance")

    val df = raw.select(raw.columns.map(c => col(c).cast(StringType)): _*)

    val confStr =
      """app {
        |  dateFormat = "yyyy-MM-dd"
        |  timestampFormat = "yyyy-MM-dd HH:mm:ss"
        |  columns = [id, name, price, age, height, is_active, created_date, updated_at, balance]
        |  schemaMapping {
        |    id           = "IntegerType"
        |    name         = "StringType"
        |    price        = "DoubleType"
        |    age          = "LongType"
        |    height       = "FloatType"
        |    is_active    = "BooleanType"
        |    created_date = "DateType:yyyy-MM-dd"
       368        |    updated_at   = "TimestampType:yyyy-MM-dd HH:mm:ss"
        |    balance      = "DecimalType:10,2"
        |  }
        |}""".stripMargin

    val conf = ConfigFactory.parseString(confStr).getConfig("app")
    val orderedCols = conf.getStringList("columns").asScala.toSeq
    val schemaMapping = conf.getConfig("schemaMapping")

    val (casted, errCnt, badDf) = CsvToAvroApp.safeCastColumns(
      df, schemaMapping, conf.getString("dateFormat"), conf.getString("timestampFormat"), spark,
      df.schema, orderedCols
    )

    assert(errCnt === 0)
    assert(badDf.isEmpty)

    val r = casted.first()
    assert(r.getInt(0) === 1)
    assert(r.getString(1) === "Alice")
    assert(r.getDouble(2) === 99.99)
    assert(r.getLong(3) === 25L)
    assert(r.getFloat(4) === 1.65f)
    assert(r.getBoolean(5) === true)
    assert(r.getAs[Date](6) === Date.valueOf("2023-01-01"))
    // UTC+5: 10:30:00 local = 05:30:00 UTC
    assert(r.getAs[Timestamp](7) === Timestamp.valueOf("2023-01-01 05:30:00"))
    assert(r.getAs[java.math.BigDecimal](8).doubleValue() === 123.45)
  }

  // =========================================================================
  // 2. Missing columns → saved to corrupted/missing_cols
  // =========================================================================
  test("missing columns → saved to corrupted/missing_cols") {
    withTempDir { base =>
      val inputDir = s"$base/input"
      val outputDir = s"$base/output"
      val corruptedDir = s"$base/corrupted"

      Files.createDirectories(Paths.get(inputDir))

      val csvContent =
        """1,Alice,99.99,25,1.65,true,2023-01-01,2023-01-01 10:30:00,123.45
          |9,Grace
          |""".stripMargin

      Files.write(Paths.get(s"$inputDir/part-00000.csv"), csvContent.getBytes)

      // PASS FULL PATHS
      CsvToAvroApp.main(Array(
        "--sourceDir", inputDir,
        "--destDir", outputDir
      ))

      val avroDf = spark.read.format("avro").load(outputDir)
      assert(avroDf.count() === 1)

      val missingDf = spark.read.json(s"$corruptedDir/missing_cols_*")
      assert(missingDf.count() === 1)
      assert(missingDf.filter($"id" === "9").count() === 1)
    }
  }

  // =========================================================================
  // 3. Casting errors → saved to corrupted/cast_errors
  // =========================================================================
  test("casting errors → saved to corrupted/cast_errors") {
    withTempDir { base =>
      val inputDir = s"$base/input"
      val outputDir = s"$base/output"
      val corruptedDir = s"$base/corrupted"

      Files.createDirectories(Paths.get(inputDir))

      val csvContent =
        """1,Alice,99.99,25,1.65,true,2023-01-01,2023-01-01 10:30:00,123.45
          |2,Bob,invalid,30,1.75,false,2023-01-02,2023-01-02 12:00:00,456.78
          |""".stripMargin

      Files.write(Paths.get(s"$inputDir/part-00000.csv"), csvContent.getBytes)

      CsvToAvroApp.main(Array(
        "--sourceDir", inputDir,
        "--destDir", outputDir
      ))

      val avroDf = spark.read.format("avro").load(outputDir)
      assert(avroDf.count() === 1)

      val castDf = spark.read.json(s"$corruptedDir/cast_errors_*")
      assert(castDf.count() === 1)
      assert(castDf.filter($"id" === "2").count() === 1)
    }
  }

  // =========================================================================
  // 4. Deduplication + partition column
  // =========================================================================
  test("deduplication + partition column") {
    withTempDir { base =>
      val inputDir = s"$base/input"
      val outputDir = s"$base/output"

      Files.createDirectories(Paths.get(inputDir))

      val csvContent =
        """1,Alice,99.99,25,1.65,true,2023-01-01,2023-01-01 10:30:00,123.45
          |1,AliceDup,99.99,25,1.65,true,2023-01-01,2023-01-01 10:30:00,123.45
          |""".stripMargin

      Files.write(Paths.get(s"$inputDir/part-00000.csv"), csvContent.getBytes)

      CsvToAvroApp.main(Array(
        "--sourceDir", inputDir,
        "--destDir", outputDir,
        "--dedupKey", "id",
        "--partitionCol", "processing_timestamp"
      ))

      val avroDf = spark.read.format("avro").load(outputDir)
      assert(avroDf.count() === 1)
      assert(avroDf.filter($"id" === 1).count() === 1)
      assert(avroDf.schema.fieldNames.contains("processing_timestamp"))
    }
  }

  // =========================================================================
  // 5. Structural corruptions
  // =========================================================================
  test("structural corruptions → saved to corrupted/structural_...") {
    withTempDir { base =>
      val inputDir = s"$base/input"
      val corruptedDir = s"$base/corrupted"

      Files.createDirectories(Paths.get(inputDir))

      val csvContent =
        """1,Alice,99.99,25,1.65,true,2023-01-01,2023-01-01 10:30:00,123.45,extra
          |2,Bob,100.00,30,1.75,false,2023-01-02,2023-01-02 12:00:00,456.78
          |""".stripMargin

      Files.write(Paths.get(s"$inputDir/part-00000.csv"), csvContent.getBytes)

      CsvToAvroApp.main(Array(
        "--sourceDir", inputDir,
        "--destDir", s"$base/output"
      ))

      val structuralDf = spark.read.json(s"$corruptedDir/structural_*")
      assert(structuralDf.count() === 1)
    }
  }

  // =========================================================================
  // 6. Full integration
  // =========================================================================
  test("integration – full pipeline with all error types") {
    withTempDir { base =>
      val inputDir = s"$base/input"
      val outputDir = s"$base/output"
      val corruptedDir = s"$base/corrupted"

      Files.createDirectories(Paths.get(inputDir))

      val csvContent =
        """1,Alice,99.99,25,1.65,true,2023-01-01,2023-01-01 10:30:00,123.45
          |2,Bob,invalid,30,1.75,false,2023-01-02,2023-01-02 12:00:00,456.78
          |9,Grace
          |null,Dilara,120.00,28,1.70,no,2023-01-0,2023-01-02 09:00:00,null
          |""".stripMargin

      Files.write(Paths.get(s"$inputDir/part-00000.csv"), csvContent.getBytes)

      CsvToAvroApp.main(Array(
        "--sourceDir", inputDir,
        "--destDir", outputDir
      ))

      val avroDf = spark.read.format("avro").load(outputDir)
      assert(avroDf.count() === 1)

      val missingDf = spark.read.json(s"$corruptedDir/missing_cols_*")
      assert(missingDf.count() === 1)

      val castDf = spark.read.json(s"$corruptedDir/cast_errors_*")
      assert(castDf.count() === 2)
    }
  }
}