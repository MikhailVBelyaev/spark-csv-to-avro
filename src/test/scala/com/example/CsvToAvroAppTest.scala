package com.example

import org.scalatest.funsuite.AnyFunSuite
import org.scalatest.BeforeAndAfterAll
import org.apache.spark.sql.{SparkSession, Row}
import org.apache.spark.sql.types._
import org.apache.spark.sql.functions._
import com.typesafe.config.ConfigFactory
import java.nio.file.{Files, Paths}
import java.sql.{Date, Timestamp}
import scala.collection.JavaConverters

/**
  * Unit + integration tests for the **new** CsvToAvroApp.
  *
  * What changed compared to the old version:
  *   • CSV is read **without header** and **all columns as String**
  *   • Column order comes from `app.columns` in config
  *   • Corrupt rows (structural / missing cols / cast errors) are written to
  *     `…/corrupted/…` as JSON
  *   • `safeCastColumns` now returns `(cleanDF, errorCount, badDF)`
  */
class CsvToAvroAppTest extends AnyFunSuite with BeforeAndAfterAll {

  System.setProperty("spark.test.active", "true")

  val spark: SparkSession = SparkSession.builder()
    .master("local[*]")
    .appName("CsvToAvroAppTest")
    .config("spark.sql.session.timeZone", "UTC+5")
    .config("spark.sql.legacy.allowNonEmptyLocationInCTAS", "true")
    .config("spark.sql.legacy.timeParserPolicy", "LEGACY")
    .getOrCreate()

  import spark.implicits._
  import org.apache.spark.sql.functions._

  override def afterAll(): Unit = spark.stop()

  /** Helper – creates a temporary directory and deletes it afterwards */
  def withTempDir(testCode: String => Unit): Unit = {
    val tmp = Files.createTempDirectory("csv_avro_test").toString
    try testCode(tmp)
    finally {
      Files.walk(Paths.get(tmp))
        .sorted(java.util.Comparator.reverseOrder())
        .forEach(Files.deleteIfExists(_))
    }
  }

  // -------------------------------------------------------------------------
  // 1. All supported types (including custom patterns)
  // -------------------------------------------------------------------------
  test("type casting – all supported types") {
    withTempDir { base =>
      val inputDir  = s"$base/input"
      val outputDir = s"$base/output"
      Files.createDirectories(Paths.get(inputDir))

      // ordered columns exactly as in config
      val ordered = Seq(
        "id","name","price","age","height","is_active",
        "created_date","updated_at","balance"
      )

      // CSV **without header**, all values are strings
      val csvLines = Seq(
        """1,Alice,99.99,25,1.65,true,2023-01-01,2023-01-01 10:30:00,123.45"""
      )
      Files.write(Paths.get(s"$inputDir/part-00000.csv"), csvLines.asJava)

      // -----------------------------------------------------------------
      // config that the app will load from classpath (src/test/resources)
      // -----------------------------------------------------------------
      val testConf =
        s"""
           |app {
           |  dateFormat      = "yyyy-MM-dd"
           |  timestampFormat = "yyyy-MM-dd HH:mm:ss"
           |  columns         = [${ordered.map(c => s""""$c"""").mkString(",")}]
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
           |}
           |""".stripMargin

      // write the config to a temporary file that will be loaded via ConfigFactory.load()
      val confPath = s"$base/application.conf"
      Files.write(Paths.get(confPath), testConf.getBytes)

      // Run the **real** main method with CLI args
      CsvToAvroApp.main(Array(
        "--sourceDir", inputDir,
        "--destDir",   outputDir,
        "--delimiter", ",",
        "--dedupKey",  "id",
        "--partitionCol", "processing_timestamp"
      ))

      // -----------------------------------------------------------------
      // Verify Avro output
      // -----------------------------------------------------------------
      val avro = spark.read.format("avro").load(outputDir)
      assert(avro.count() === 1)

      val r = avro.first()
      assert(r.getAs[Int]("id")            === 1)
      assert(r.getAs[String]("name")       === "Alice")
      assert(r.getAs[Double]("price")      === 99.99)
      assert(r.getAs[Long]("age")          === 25L)
      assert(r.getAs[Float]("height")      === 1.65f)
      assert(r.getAs[Boolean]("is_active"))
      assert(r.getAs[Date]("created_date") === Date.valueOf("2023-01-01"))
      assert(r.getAs[Timestamp]("updated_at") === Timestamp.valueOf("2023-01-01 10:30:00"))
      assert(r.getAs[java.math.BigDecimal]("balance") === new java.math.BigDecimal("123.45"))
    }
  }

  // -------------------------------------------------------------------------
  // 2. Date / timestamp with custom patterns
  // -------------------------------------------------------------------------
  test("date & timestamp – different format patterns") {
    withTempDir { base =>
      val inputDir  = s"$base/input"
      val outputDir = s"$base/output"
      Files.createDirectories(Paths.get(inputDir))

      val ordered = Seq("created_date","alt_date","updated_at","alt_timestamp")
      val csvLines = Seq(
        """2023-01-01,01/02/2023,2023-01-01 10:30:00,01-02-2023 12:00:00"""
      )
      Files.write(Paths.get(s"$inputDir/part-00000.csv"), csvLines.asJava)

      val testConf =
        s"""
           |app {
           |  dateFormat      = "yyyy-MM-dd"
           |  timestampFormat = "yyyy-MM-dd HH:mm:ss"
           |  columns         = [${ordered.map(c => s""""$c"""").mkString(",")}]
           |  schemaMapping {
           |    created_date = "DateType:yyyy-MM-dd"
           |    alt_date     = "DateType:dd/MM/yyyy"
           |    updated_at   = "TimestampType:yyyy-MM-dd HH:mm:ss"
           |    alt_timestamp= "TimestampType:dd-MM-yyyy HH:mm:ss"
           |  }
           |}
           |""".stripMargin

      val confPath = s"$base/application.conf"
      Files.write(Paths.get(confPath), testConf.getBytes)

      CsvToAvroApp.main(Array(
        "--sourceDir", inputDir,
        "--destDir",   outputDir,
        "--delimiter", ","
      ))

      val avro = spark.read.format("avro").load(outputDir)
      val r = avro.first()
      assert(r.getAs[Date]("created_date")   === Date.valueOf("2023-01-01"))
      assert(r.getAs[Date]("alt_date")       === Date.valueOf("2023-02-01"))
      assert(r.getAs[Timestamp]("updated_at")=== Timestamp.valueOf("2023-01-01 10:30:00"))
      assert(r.getAs[Timestamp]("alt_timestamp") === Timestamp.valueOf("2023-02-01 12:00:00"))
    }
  }

  // -------------------------------------------------------------------------
  // 3. Invalid data → null + corrupted JSON files
  // -------------------------------------------------------------------------
  test("invalid data → null + corrupted JSON files") {
    withTempDir { base =>
      val inputDir  = s"$base/input"
      val outputDir = s"$base/output"
      Files.createDirectories(Paths.get(inputDir))

      val ordered = Seq(
        "id","name","price","age","height","is_active",
        "created_date","updated_at","balance"
      )
      val csvLines = Seq(
        """invalid,Alice,invalid,invalid,invalid,invalid,invalid,invalid,invalid"""
      )
      Files.write(Paths.get(s"$inputDir/part-00000.csv"), csvLines.asJava)

      val testConf =
        s"""
           |app {
           |  dateFormat      = "yyyy-MM-dd"
           |  timestampFormat = "yyyy-MM-dd HH:mm:ss"
           |  columns         = [${ordered.map(c => s""""$c"""").mkString(",")}]
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
           |}
           |""".stripMargin

      val confPath = s"$base/application.conf"
      Files.write(Paths.get(confPath), testConf.getBytes)

      CsvToAvroApp.main(Array(
        "--sourceDir", inputDir,
        "--destDir",   outputDir,
        "--delimiter", ",",
        "--dedupKey",  "id"
      ))

      // clean Avro should be empty (all rows failed casting)
      val avro = spark.read.format("avro").load(outputDir)
      assert(avro.isEmpty)

      // corrupted JSON must contain the original row (8 cast errors)
      val corruptDir = Paths.get(s"$base/corrupted")
      val castErrorFiles = Files.list(corruptDir)
        .filter(p => p.toString.contains("cast_errors"))
        .findFirst()
        .orElse(fail("No cast_errors file found"))

      val bad = spark.read.json(castErrorFiles.toString)
      assert(bad.count() === 1)
      val row = bad.first()
      assert(row.getAs[String]("name") === "Alice")
      // all other fields are null
      assert(row.isNullAt(row.fieldIndex("id")))
      assert(row.isNullAt(row.fieldIndex("price")))
      // … etc.
    }
  }

  // -------------------------------------------------------------------------
  // 4. Deduplication + partition column
  // -------------------------------------------------------------------------
  test("deduplication & partition column") {
    withTempDir { base =>
      val inputDir  = s"$base/input"
      val outputDir = s"$base/output"
      Files.createDirectories(Paths.get(inputDir))

      val ordered = Seq("id","name")
      val csvLines = Seq(
        """1,Alice""",
        """1,Bob""",
        """,Charlie"""   // missing id → will be filtered
      )
      Files.write(Paths.get(s"$inputDir/part-00000.csv"), csvLines.asJava)

      val testConf =
        s"""
           |app {
           |  columns = ["id","name"]
           |  schemaMapping {
           |    id   = "IntegerType"
           |    name = "StringType"
           |  }
           |}
           |""".stripMargin

      val confPath = s"$base/application.conf"
      Files.write(Paths.get(confPath), testConf.getBytes)

      CsvToAvroApp.main(Array(
        "--sourceDir", inputDir,
        "--destDir",   outputDir,
        "--delimiter", ",",
        "--dedupKey",  "id",
        "--partitionCol", "processing_timestamp"
      ))

      val avro = spark.read.format("avro").load(outputDir)
      assert(avro.count() === 1)                 // deduped
      assert(avro.filter($"id" === 1).count() === 1)
      assert(avro.schema.exists(_.name == "processing_timestamp"))
      assert(avro.schema("processing_timestamp").dataType === TimestampType)
    }
  }

  // -------------------------------------------------------------------------
  // 5. Integration – full pipeline (CSV → Avro + corrupted JSON)
  // -------------------------------------------------------------------------
  test("integration – full pipeline") {
    withTempDir { base =>
      val inputDir  = s"$base/input"
      val outputDir = s"$base/output"
      Files.createDirectories(Paths.get(inputDir))

      val ordered = Seq(
        "id","name","price","age","height","is_active",
        "created_date","updated_at","balance"
      )
      val csvLines = Seq(
        """1,Alice,99.99,25,1.65,true,2023-01-01,2023-01-01 10:30:00,123.45""",
        """2,Bob,invalid,30,1.75,false,2023-01-02,2023-01-02 12:00:00,456.78"""
      )
      Files.write(Paths.get(s"$inputDir/part-00000.csv"), csvLines.asJava)

      val testConf =
        s"""
           |app {
           |  dateFormat      = "yyyy-MM-dd"
           |  timestampFormat = "yyyy-MM-dd HH:mm:ss"
           |  columns         = [${ordered.map(c => s""""$c"""").mkString(",")}]
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
           |}
           |""".stripMargin

      val confPath = s"$base/application.conf"
      Files.write(Paths.get(confPath), testConf.getBytes)

      CsvToAvroApp.main(Array(
        "--sourceDir", inputDir,
        "--destDir",   outputDir,
        "--delimiter", ",",
        "--dedupKey",  "id",
        "--partitionCol", "processing_timestamp"
      ))

      // ----- clean Avro -----
      val avro = spark.read.format("avro").load(outputDir)
      assert(avro.count() === 2)

      val good = avro.filter($"price".isNotNull)
      assert(good.count() === 1)
      assert(good.filter($"name" === "Alice").count() === 1)

      val badPrice = avro.filter($"price".isNull)
      assert(badPrice.count() === 1)
      assert(badPrice.filter($"name" === "Bob").count() === 1)

      // ----- corrupted JSON (cast errors) -----
      val corruptDir = Paths.get(s"$base/corrupted")
      val castErrorFile = Files.list(corruptDir)
        .filter(p => p.toString.contains("cast_errors"))
        .findFirst()
        .get

      val badDf = spark.read.json(castErrorFile.toString)
      assert(badDf.count() === 1)
      val badRow = badDf.first()
      assert(badRow.getAs[String]("name") === "Bob")
      assert(badRow.isNullAt(badRow.fieldIndex("price")))
    }
  }
}