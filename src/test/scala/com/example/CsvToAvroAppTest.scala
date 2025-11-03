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

/**
  * Unit + integration tests for FINAL CsvToAvroApp.
  *
  * Now tests:
  *   • Missing columns → saved to /corrupted/missing_cols_...
  *   • Structural corruptions → saved to /corrupted/structural_...
  *   • Casting errors → saved to /corrupted/cast_errors_...
  *   • Clean Avro output → only valid rows
  *   • Deduplication + partition column
  *   • Timezone UTC+5 (same as prod)
  */
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
        |    updated_at   = "TimestampType:yyyy-MM-dd HH:mm:ss"
        |    balance      = "DecimalType:10,2"
        |  }
        |}""".stripMargin

    val conf = ConfigFactory.parseString(confStr).getConfig("app")
    val orderedCols = conf.getStringList("columns").asScala.toSeq
    val schemaMapping = conf.getConfig("schemaMapping")

    // Simulate full pipeline up to safeCastColumns
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
    assert(r.getAs[Timestamp](7) === Timestamp.valueOf("2023-01-01 10:30:00"))
    assert(r.getAs[java.math.BigDecimal](8).doubleValue() === 123.45)
  }

  // =========================================================================
  // 2. Missing columns → saved to corrupted/missing_cols_...
  // =========================================================================
  test("missing columns → saved to corrupted/missing_cols") {
    withTempDir { base =>
      val inputDir = s"$base/input"
      val outputDir = s"$base/output"
      val corruptedDir = s"$base/corrupted"

      Files.createDirectories(Paths.get(inputDir))
      Files.createDirectories(Paths.get(corruptedDir))

      // Row with only 2 fields: 9,Grace
      val csvContent =
        """1,Alice,99.99,25,1.65,true,2023-01-01,2023-01-01 10:30:00,123.45
          |9,Grace
          |""".stripMargin

      Files.write(Paths.get(s"$inputDir/part-00000.csv"), csvContent.getBytes)

      // Run full app
      CsvToAvroApp.main(Array(
        "--sourceDir", "input",
        "--destDir", "output",
        "--delimiter", ","
      ).map(a => a.replace("input", inputDir).replace("output", outputDir)))

      // 1. Avro: should have 1 row
      val avroDf = spark.read.format("avro").load(outputDir)
      assert(avroDf.count() === 1)
      assert(avroDf.filter($"id" === 1).count() === 1)

      // 2. missing_cols: should have 1 row
      val missingFiles = Files.list(Paths.get(corruptedDir))
        .filter(p => p.toString.contains("missing_cols"))
        .findFirst()
      assert(missingFiles.isPresent)

      val missingDf = spark.read.json(missingFiles.get().toString)
      assert(missingDf.count() === 1)
      val row = missingDf.first()
      assert(row.getString(row.fieldIndex("id")) === "9")
      assert(row.getString(row.fieldIndex("name")) === "Grace")
      assert(row.isNullAt(row.fieldIndex("price")))
    }
  }

  // =========================================================================
  // 3. Casting errors → saved to corrupted/cast_errors_...
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
        "--sourceDir", "input",
        "--destDir", "output"
      ).map(a => a.replace("input", inputDir).replace("output", outputDir)))

      // Clean Avro: 1 row
      val avroDf = spark.read.format("avro").load(outputDir)
      assert(avroDf.count() === 1)

      // cast_errors: 1 row
      val castFiles = Files.list(Paths.get(corruptedDir))
        .filter(p => p.toString.contains("cast_errors"))
        .findFirst()
      assert(castFiles.isPresent)

      val castDf = spark.read.json(castFiles.get().toString)
      assert(castDf.count() === 1)
      val row = castDf.first()
      assert(row.getString(row.fieldIndex("id")) === "2")
      assert(row.getString(row.fieldIndex("price")) === "invalid")
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
        "--sourceDir", "input",
        "--destDir", "output",
        "--dedupKey", "id",
        "--partitionCol", "processing_timestamp"
      ).map(a => a.replace("input", inputDir).replace("output", outputDir)))

      val avroDf = spark.read.format("avro").load(outputDir)
      assert(avroDf.count() === 1)
      assert(avroDf.filter($"id" === 1).count() === 1)
      assert(avroDf.schema.fieldNames.contains("processing_timestamp"))
    }
  }

  // =========================================================================
  // 5. Structural corruptions (malformed CSV)
  // =========================================================================
  test("structural corruptions → saved to corrupted/structural_...") {
    withTempDir { base =>
      val inputDir = s"$base/input"
      val corruptedDir = s"$base/corrupted"

      Files.createDirectories(Paths.get(inputDir))

      // Malformed: too many fields
      val csvContent =
        """1,Alice,99.99,25,1.65,true,2023-01-01,2023-01-01 10:30:00,123.45,extra
          |2,Bob,100.00,30,1.75,false,2023-01-02,2023-01-02 12:00:00,456.78
          |""".stripMargin

      Files.write(Paths.get(s"$inputDir/part-00000.csv"), csvContent.getBytes)

      CsvToAvroApp.main(Array(
        "--sourceDir", "input",
        "--destDir", "output"
      ).map(a => a.replace("input", inputDir)))

      val structuralFiles = Files.list(Paths.get(corruptedDir))
        .filter(p => p.toString.contains("structural"))
        .findFirst()
      assert(structuralFiles.isPresent)

      val structuralDf = spark.read.json(structuralFiles.get().toString)
      assert(structuralDf.count() === 1)
      assert(structuralDf.filter($"_corrupt_record".contains("extra")).count() === 1)
    }
  }

  // =========================================================================
  // 6. Full integration: missing + cast errors + clean output
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
        "--sourceDir", "input",
        "--destDir", "output"
      ).map(a => a.replace("input", inputDir).replace("output", outputDir)))

      // Clean Avro: 1 row
      val avroDf = spark.read.format("avro").load(outputDir)
      assert(avroDf.count() === 1)
      assert(avroDf.filter($"id" === 1).count() === 1)

      // missing_cols: 1 row (9,Grace)
      val missingDf = spark.read.json(s"$corruptedDir/missing_cols_*")
      assert(missingDf.count() === 1)
      assert(missingDf.filter($"id" === "9").count() === 1)

      // cast_errors: 2 rows (Bob + Dilara)
      val castDf = spark.read.json(s"$corruptedDir/cast_errors_*")
      assert(castDf.count() === 2)
    }
  }
}