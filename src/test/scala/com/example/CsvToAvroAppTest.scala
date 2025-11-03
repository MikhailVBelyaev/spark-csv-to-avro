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
        |    updated_at   = "TimestampType:yyyy-MM-dd HH:mm:ss"
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

    // FIX: Use Spark to parse expected timestamp in UTC+5
    val expectedTs = spark.sql(
      "SELECT to_timestamp('2023-01-01 10:30:00', 'yyyy-MM-dd HH:mm:ss') AS ts"
    ).first().getTimestamp(0)

    assert(r.getAs[Timestamp](7) === expectedTs)
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

      // FIX: Pass relative path, app will resolve to /app/input
      CsvToAvroApp.main(Array(
        "--sourceDir", "input",
        "--destDir", "output"
      ))

      // Copy files into container's /app
      import sys.process._
      s"docker cp $inputDir spark_csv_test:/app/input".!
      s"docker cp $outputDir spark_csv_test:/app/output".!
      s"docker cp $corruptedDir spark_csv_test:/app/corrupted".!

      // Re-run inside container
      s"docker exec spark_csv_test bash -c 'cd /app && java -cp target/scala-2.12/test-classes:... CsvToAvroApp --sourceDir input --destDir output'".!

      val avroDf = spark.read.format("avro").load(s"$outputDir")
      assert(avroDf.count() === 1)

      val missingDf = spark.read.json(s"$corruptedDir/missing_cols_*")
      assert(missingDf.count() === 1)
      assert(missingDf.filter($"id" === "9").count() === 1)
    }
  }