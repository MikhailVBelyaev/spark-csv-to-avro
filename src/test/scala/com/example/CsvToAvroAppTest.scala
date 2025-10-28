package com.example

import org.scalatest.funsuite.AnyFunSuite
import org.scalatest.BeforeAndAfterAll
import org.apache.spark.sql.{SparkSession, DataFrame, Row}
import org.apache.spark.sql.types._
import com.typesafe.config.ConfigFactory
import java.nio.file.{Files, Paths}
import java.sql.{Date, Timestamp}

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
  import org.apache.spark.sql.functions._

  override def afterAll(): Unit = {
    spark.stop()
  }

  // Helper to create a temporary directory
  def withTempDir(testCode: String => Unit): Unit = {
    val tempDir = Files.createTempDirectory("spark_test").toString
    try {
      testCode(tempDir)
    } finally {
      // Clean up recursively
      Files.walk(Paths.get(tempDir))
        .sorted(java.util.Comparator.reverseOrder())
        .forEach(Files.deleteIfExists(_))
    }
  }

  test("Type casting for all supported types") {
    val df = Seq((
      "1",           // id: IntegerType
      "Alice",       // name: StringType
      "99.99",       // price: DoubleType
      "25",          // age: LongType
      "1.65",        // height: FloatType
      "true",        // is_active: BooleanType
      "2023-01-01",  // created_date: DateType
      "2023-01-01 10:30:00", // updated_at: TimestampType
      "123.45"       // balance: DecimalType(10,2)
    )).toDF("id", "name", "price", "age", "height", "is_active", "created_date", "updated_at", "balance")

    val confStr =
      """
        |app {
        | schemaMapping {
        |   id = "IntegerType"
        |   name = "StringType"
        |   price = "DoubleType"
        |   age = "LongType"
        |   height = "FloatType"
        |   is_active = "BooleanType"
        |   created_date = "DateType:yyyy-MM-dd"
        |   updated_at = "TimestampType:yyyy-MM-dd HH:mm:ss"
        |   balance = "DecimalType:10,2"
        | }
        |}
        |""".stripMargin

    val conf = ConfigFactory.parseString(confStr).getConfig("app.schemaMapping")
    val (res, errorCount) = CsvToAvroApp.safeCastColumns(df, conf, "yyyy-MM-dd", "yyyy-MM-dd HH:mm:ss")

    // Verify schema
    val schema = res.schema
    assert(schema("id").dataType == IntegerType)
    assert(schema("name").dataType == StringType)
    assert(schema("price").dataType == DoubleType)
    assert(schema("age").dataType == LongType)
    assert(schema("height").dataType == FloatType)
    assert(schema("is_active").dataType == BooleanType)
    assert(schema("created_date").dataType == DateType)
    assert(schema("updated_at").dataType == TimestampType)
    assert(schema("balance").dataType == DecimalType(10, 2))

    // Verify data
    val row = res.first()
    assert(row.getAs[Int]("id") == 1)
    assert(row.getAs[String]("name") == "Alice")
    assert(row.getAs[Double]("price") == 99.99)
    assert(row.getAs[Long]("age") == 25L)
    assert(row.getAs[Float]("height") == 1.65f)
    assert(row.getAs[Boolean]("is_active"))
    assert(row.getAs[Date]("created_date") == Date.valueOf("2023-01-01"))
    assert(row.getAs[Timestamp]("updated_at") == Timestamp.valueOf("2023-01-01 10:30:00"))
    assert(row.getAs[java.math.BigDecimal]("balance") == new java.math.BigDecimal("123.45"))
    assert(errorCount == 0)
  }

  test("Date and timestamp parsing with different formats") {
    val df = Seq((
      "2023-01-01",       // created_date: DateType (yyyy-MM-dd)
      "01/02/2023",       // alt_date: DateType (dd/MM/yyyy)
      "2023-01-01 10:30:00", // updated_at: TimestampType (yyyy-MM-dd HH:mm:ss)
      "01-02-2023 12:00:00"  // alt_timestamp: TimestampType (dd-MM-yyyy HH:mm:ss)
    )).toDF("created_date", "alt_date", "updated_at", "alt_timestamp")

    val confStr =
      """
        |app {
        | schemaMapping {
        |   created_date = "DateType:yyyy-MM-dd"
        |   alt_date = "DateType:dd/MM/yyyy"
        |   updated_at = "TimestampType:yyyy-MM-dd HH:mm:ss"
        |   alt_timestamp = "TimestampType:dd-MM-yyyy HH:mm:ss"
        | }
        |}
        |""".stripMargin

    val conf = ConfigFactory.parseString(confStr).getConfig("app.schemaMapping")
    val (res, errorCount) = CsvToAvroApp.safeCastColumns(df, conf, "yyyy-MM-dd", "yyyy-MM-dd HH:mm:ss")

    // Verify schema
    assert(res.schema("created_date").dataType == DateType)
    assert(res.schema("alt_date").dataType == DateType)
    assert(res.schema("updated_at").dataType == TimestampType)
    assert(res.schema("alt_timestamp").dataType == TimestampType)

    // Verify data
    val row = res.first()
    assert(row.getAs[Date]("created_date") == Date.valueOf("2023-01-01"))
    assert(row.getAs[Date]("alt_date") == Date.valueOf("2023-02-01"))
    assert(row.getAs[Timestamp]("updated_at") == Timestamp.valueOf("2023-01-01 10:30:00"))
    assert(row.getAs[Timestamp]("alt_timestamp") == Timestamp.valueOf("2023-02-01 12:00:00"))
    assert(errorCount == 0)
  }

  test("Timezone debug") {
    val ts = Timestamp.valueOf("2023-01-01 10:30:00")
    println(s"Java Timestamp: $ts")
    println(s"Epoch: ${ts.getTime}")

    val df = Seq(("2023-01-01 10:30:00")).toDF("ts")
    val parsed = df.withColumn("p", to_timestamp(col("ts"), "yyyy-MM-dd HH:mm:ss"))
    parsed.show()

    val row = parsed.first()
    val sparkTs = row.getTimestamp(1)
    println(s"Spark Timestamp: $sparkTs")
    println(s"Epoch: ${sparkTs.getTime}")
  }

  test("Type casting with invalid data") {
    val df = Seq((
      "invalid",     // id: IntegerType (fails)
      "Alice",       // name: StringType
      "invalid",     // price: DoubleType (fails)
      "invalid",     // age: LongType (fails)
      "invalid",     // height: FloatType (fails)
      "invalid",     // is_active: BooleanType (fails)
      "invalid",     // created_date: DateType (fails)
      "invalid",     // updated_at: TimestampType (fails)
      "invalid"      // balance: DecimalType(10,2) (fails)
    )).toDF("id", "name", "price", "age", "height", "is_active", "created_date", "updated_at", "balance")

    val confStr =
      """
        |app {
        | schemaMapping {
        |   id = "IntegerType"
        |   name = "StringType"
        |   price = "DoubleType"
        |   age = "LongType"
        |   height = "FloatType"
        |   is_active = "BooleanType"
        |   created_date = "DateType:yyyy-MM-dd"
        |   updated_at = "TimestampType:yyyy-MM-dd HH:mm:ss"
        |   balance = "DecimalType:10,2"
        | }
        |}
        |""".stripMargin

    val conf = ConfigFactory.parseString(confStr).getConfig("app.schemaMapping")
    val (res, errorCount) = CsvToAvroApp.safeCastColumns(df, conf, "yyyy-MM-dd", "yyyy-MM-dd HH:mm:ss")

    // Verify schema
    assert(res.schema("id").dataType == IntegerType)
    assert(res.schema("name").dataType == StringType)

    // Verify data: invalid casts become null
    val row = res.first()
    assert(row.isNullAt(row.fieldIndex("id")))
    assert(row.getAs[String]("name") == "Alice")
    assert(row.isNullAt(row.fieldIndex("price")))
    assert(row.isNullAt(row.fieldIndex("age")))
    assert(row.isNullAt(row.fieldIndex("height")))
    assert(row.isNullAt(row.fieldIndex("is_active")))
    assert(row.isNullAt(row.fieldIndex("created_date")))
    assert(row.isNullAt(row.fieldIndex("updated_at")))
    assert(row.isNullAt(row.fieldIndex("balance")))
    assert(errorCount == 8) // 8 failed casts
  }

  test("Deduplication and validation in process") {
    val schema = StructType(Seq(
      StructField("id", IntegerType, nullable = true),
      StructField("name", StringType, nullable = true)
    ))
    val data = Seq(
      Row(1, "Alice"),
      Row(1, "Bob"),
      Row(null, "Charlie")
    )
    val df = spark.createDataFrame(
      spark.sparkContext.parallelize(data),
      schema
    )

    val confStr =
      """
        |app {
        | schemaMapping {
        |   id = "IntegerType"
        |   name = "StringType"
        | }
        |}
        |""".stripMargin

    val conf = ConfigFactory.parseString(confStr).getConfig("app.schemaMapping")
    val res = CsvToAvroApp.process(df, conf, "id", "processing_timestamp", "yyyy-MM-dd", "yyyy-MM-dd HH:mm:ss")

    // Verify: 1 record (deduped, null id filtered)
    assert(res.count() == 1)
    assert(res.filter($"id" === 1).count() == 1)
    assert(res.schema("processing_timestamp").dataType == TimestampType)
  }

  test("Integration test: Full pipeline") {
    withTempDir { tempDir =>
      // Use RELATIVE paths that will be prefixed with /app by the app
      val inputRelPath = s"test-input-${java.util.UUID.randomUUID().toString.take(8)}"
      val outputRelPath = s"test-output-${java.util.UUID.randomUUID().toString.take(8)}"

      val inputPath = s"/app/$inputRelPath"
      val outputPath = s"/app/$outputRelPath"

      // Clean up any old test dirs
      try { java.nio.file.Files.walk(Paths.get(inputPath)).sorted(java.util.Comparator.reverseOrder()).forEach(Files.deleteIfExists(_)) } catch { case _: Throwable => }
      try { java.nio.file.Files.walk(Paths.get(outputPath)).sorted(java.util.Comparator.reverseOrder()).forEach(Files.deleteIfExists(_)) } catch { case _: Throwable => }

      Files.createDirectories(Paths.get(inputPath))

      val df = Seq(
        ("1", "Alice", "99.99", "25", "1.65", "true", "2023-01-01", "2023-01-01 10:30:00", "123.45"),
        ("2", "Bob", "invalid", "30", "1.75", "false", "2023-01-02", "2023-01-02 12:00:00", "456.78")
      ).toDF("id", "name", "price", "age", "height", "is_active", "created_date", "updated_at", "balance")

      df.write
        .mode("overwrite")
        .option("header", "true")
        .option("delimiter", ",")
        .csv(inputPath)

      // Pass RELATIVE paths — app will add /app/
      CsvToAvroApp.main(Array(
        "--sourceDir", inputRelPath,
        "--destDir", outputRelPath,
        "--delimiter", ",",
        "--dedupKey", "id",
        "--partitionCol", "processing_timestamp"
      ))

      val outputDf = spark.read.format("avro").load(outputPath)
      assert(outputDf.count() == 2)
      assert(outputDf.filter($"price".isNull).count() == 1)
    }
  }
}