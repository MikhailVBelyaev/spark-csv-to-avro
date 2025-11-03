package com.example

import org.scalatest.funsuite.AnyFunSuite
import org.scalatest.BeforeAndAfterAll
import org.apache.spark.sql.{SparkSession, Row}
import org.apache.spark.sql.types._
import org.apache.spark.sql.functions._
import com.typesafe.config.ConfigFactory
import java.nio.file.{Files, Paths}
import java.sql.{Date, Timestamp}

/**
  * Unit + integration tests for CsvToAvroApp.
  *
  * What changed compared to the previous version:
  *   • Spark session timezone = "UTC+5" (same as production)
  *   • import org.apache.spark.sql.functions._
  *   • Every test DataFrame is forced to StringType before casting
  *   • No inferSchema → safeCastColumns always sees raw strings
  */
class CsvToAvroAppTest extends AnyFunSuite with BeforeAndAfterAll {

  System.setProperty("spark.test.active", "true")

  val spark: SparkSession = SparkSession.builder()
    .master("local[*]")
    .appName("TestApp")
    .config("spark.sql.session.timeZone", "UTC+5")          // <-- SAME AS MAIN APP
    .config("spark.sql.legacy.allowNonEmptyLocationInCTAS", "true")
    .config("spark.sql.legacy.timeParserPolicy", "LEGACY")
    .getOrCreate()

  import spark.implicits._
  import org.apache.spark.sql.functions._                 // <-- REQUIRED

  override def afterAll(): Unit = spark.stop()

  /** Helper – creates a temporary directory and deletes it afterwards */
  def withTempDir(testCode: String => Unit): Unit = {
    val tmp = Files.createTempDirectory("spark_test").toString
    try testCode(tmp)
    finally {
      Files.walk(Paths.get(tmp))
        .sorted(java.util.Comparator.reverseOrder())
        .forEach(Files.deleteIfExists(_))
    }
  }

  // -------------------------------------------------------------------------
  // 1. All supported types
  // -------------------------------------------------------------------------
  test("type casting – all supported types") {
    val raw = Seq((
      "1", "Alice", "99.99", "25", "1.65", "true",
      "2023-01-01", "2023-01-01 10:30:00", "123.45"
    )).toDF("id","name","price","age","height","is_active",
            "created_date","updated_at","balance")

    // <-- FORCE STRING
    val df = raw.select(raw.columns.map(c => col(c).cast(StringType)): _*)

    val confStr =
      """app { schemaMapping {
        |  id          = "IntegerType"
        |  name        = "StringType"
        |  price       = "DoubleType"
        |  age         = "LongType"
        |  height      = "FloatType"
        |  is_active   = "BooleanType"
        |  created_date= "DateType:yyyy-MM-dd"
        |  updated_at  = "TimestampType:yyyy-MM-dd HH:mm:ss"
        |  balance     = "DecimalType:10,2"
        |}}""".stripMargin

    val conf = ConfigFactory.parseString(confStr).getConfig("app.schemaMapping")
    val (casted, errCnt) = CsvToAvroApp.safeCastColumns(df, conf,
                                 "yyyy-MM-dd", "yyyy-MM-dd HH:mm:ss")

    // schema
    assert(casted.schema("id").dataType          === IntegerType)
    assert(casted.schema("name").dataType        === StringType)
    assert(casted.schema("price").dataType       === DoubleType)
    assert(casted.schema("age").dataType         === LongType)
    assert(casted.schema("height").dataType      === FloatType)
    assert(casted.schema("is_active").dataType   === BooleanType)
    assert(casted.schema("created_date").dataType=== DateType)
    assert(casted.schema("updated_at").dataType  === TimestampType)
    assert(casted.schema("balance").dataType     === DecimalType(10,2))

    // data
    val r = casted.first()
    assert(r.getAs[Int]("id")            === 1)
    assert(r.getAs[String]("name")       === "Alice")
    assert(r.getAs[Double]("price")      === 99.99)
    assert(r.getAs[Long]("age")          === 25L)
    assert(r.getAs[Float]("height")      === 1.65f)
    assert(r.getAs[Boolean]("is_active"))
    assert(r.getAs[Date]("created_date") === Date.valueOf("2023-01-01"))
    assert(r.getAs[Timestamp]("updated_at") === Timestamp.valueOf("2023-01-01 10:30:00"))
    assert(r.getAs[java.math.BigDecimal]("balance") === new java.math.BigDecimal("123.45"))
    assert(errCnt === 0)
  }

  // -------------------------------------------------------------------------
  // 2. Date / timestamp with custom patterns
  // -------------------------------------------------------------------------
  test("date & timestamp – different format patterns") {
    val raw = Seq((
      "2023-01-01", "01/02/2023", "2023-01-01 10:30:00", "01-02-2023 12:00:00"
    )).toDF("created_date","alt_date","updated_at","alt_timestamp")

    val df = raw.select(raw.columns.map(c => col(c).cast(StringType)): _*)

    val confStr =
      """app { schemaMapping {
        |  created_date = "DateType:yyyy-MM-dd"
        |  alt_date     = "DateType:dd/MM/yyyy"
        |  updated_at   = "TimestampType:yyyy-MM-dd HH:mm:ss"
        |  alt_timestamp= "TimestampType:dd-MM-yyyy HH:mm:ss"
        |}}""".stripMargin

    val conf = ConfigFactory.parseString(confStr).getConfig("app.schemaMapping")
    val (casted, errCnt) = CsvToAvroApp.safeCastColumns(df, conf,
                                 "yyyy-MM-dd", "yyyy-MM-dd HH:mm:ss")

    val r = casted.first()
    assert(r.getAs[Date]("created_date")   === Date.valueOf("2023-01-01"))
    assert(r.getAs[Date]("alt_date")       === Date.valueOf("2023-02-01"))
    assert(r.getAs[Timestamp]("updated_at")=== Timestamp.valueOf("2023-01-01 10:30:00"))
    assert(r.getAs[Timestamp]("alt_timestamp") === Timestamp.valueOf("2023-02-01 12:00:00"))
    assert(errCnt === 0)
  }

  // -------------------------------------------------------------------------
  // OPTIONAL: 2.1. Keep debug test to verify timezone — can be removed later
  // -------------------------------------------------------------------------
  test("Timezone debug") {
    val ts = Timestamp.valueOf("2023-01-01 10:30:00")
    println(s"Java Timestamp: $ts")
    println(s"Epoch: ${ts.getTime}")

    val df = Seq(("2023-01-01 10:30:00")).toDF("ts").withColumn("ts", col("ts").cast(StringType))
    val parsed = df.withColumn("p", to_timestamp(col("ts"), "yyyy-MM-dd HH:mm:ss"))
    parsed.show()

    val row = parsed.first()
    val sparkTs = row.getTimestamp(1)
    println(s"Spark Timestamp: $sparkTs")
    println(s"Epoch: ${sparkTs.getTime}")
  }

  // -------------------------------------------------------------------------
  // 3. Invalid data → null + error count
  // -------------------------------------------------------------------------
  test("invalid data → null + error count") {
    val raw = Seq((
      "invalid","Alice","invalid","invalid","invalid","invalid",
      "invalid","invalid","invalid"
    )).toDF("id","name","price","age","height","is_active",
            "created_date","updated_at","balance")

    val df = raw.select(raw.columns.map(c => col(c).cast(StringType)): _*)

    val confStr =
      """app { schemaMapping {
        |  id          = "IntegerType"
        |  name        = "StringType"
        |  price       = "DoubleType"
        |  age         = "LongType"
        |  height      = "FloatType"
        |  is_active   = "BooleanType"
        |  created_date= "DateType:yyyy-MM-dd"
        |  updated_at  = "TimestampType:yyyy-MM-dd HH:mm:ss"
        |  balance     = "DecimalType:10,2"
        |}}""".stripMargin

    val conf = ConfigFactory.parseString(confStr).getConfig("app.schemaMapping")
    val (casted, errCnt) = CsvToAvroApp.safeCastColumns(df, conf,
                                 "yyyy-MM-dd", "yyyy-MM-dd HH:mm:ss")

    val r = casted.first()
    assert(r.isNullAt(r.fieldIndex("id")))
    assert(r.getAs[String]("name") === "Alice")
    assert(r.isNullAt(r.fieldIndex("price")))
    assert(r.isNullAt(r.fieldIndex("age")))
    assert(r.isNullAt(r.fieldIndex("height")))
    assert(r.isNullAt(r.fieldIndex("is_active")))
    assert(r.isNullAt(r.fieldIndex("created_date")))
    assert(r.isNullAt(r.fieldIndex("updated_at")))
    assert(r.isNullAt(r.fieldIndex("balance")))
    assert(errCnt === 8)   // 8 columns failed
  }

  // -------------------------------------------------------------------------
  // 4. Deduplication + partition column
  // -------------------------------------------------------------------------
  test("deduplication & partition column") {
    val schema = StructType(Seq(
      StructField("id",   IntegerType, nullable = true),
      StructField("name", StringType,  nullable = true)
    ))
    val rows = Seq(
      Row(1, "Alice"),
      Row(1, "Bob"),
      Row(null, "Charlie")
    )
    val df = spark.createDataFrame(spark.sparkContext.parallelize(rows), schema)

    val confStr =
      """app { schemaMapping {
        |  id   = "IntegerType"
        |  name = "StringType"
        |}}""".stripMargin

    val conf = ConfigFactory.parseString(confStr).getConfig("app.schemaMapping")
    val processed = CsvToAvroApp.process(df, conf, "id",
                                         "processing_timestamp",
                                         "yyyy-MM-dd", "yyyy-MM-dd HH:mm:ss")

    assert(processed.count() === 1)
    assert(processed.filter($"id" === 1).count() === 1)
    assert(processed.schema("processing_timestamp").dataType === TimestampType)
  }

  // -------------------------------------------------------------------------
  // 5. Integration – full pipeline (CSV → Avro)
  // -------------------------------------------------------------------------
  test("integration – full pipeline") {
    withTempDir { _ =>
      val in  = s"test-input-${java.util.UUID.randomUUID().toString.take(8)}"
      val out = s"test-output-${java.util.UUID.randomUUID().toString.take(8)}"
      val inPath  = s"/app/$in"
      val outPath = s"/app/$out"

      // clean any leftovers
      try Files.walk(Paths.get(inPath)).sorted(java.util.Comparator.reverseOrder())
           .forEach(Files.deleteIfExists(_))
      catch { case _: Throwable => }
      try Files.walk(Paths.get(outPath)).sorted(java.util.Comparator.reverseOrder())
           .forEach(Files.deleteIfExists(_))
      catch { case _: Throwable => }

      Files.createDirectories(Paths.get(inPath))

      val csv = Seq(
        ("1","Alice","99.99","25","1.65","true","2023-01-01","2023-01-01 10:30:00","123.45"),
        ("2","Bob","invalid","30","1.75","false","2023-01-02","2023-01-02 12:00:00","456.78")
      ).toDF("id","name","price","age","height","is_active",
             "created_date","updated_at","balance")

      csv.write.mode("overwrite")
        .option("header","true").option("delimiter",",")
        .csv(inPath)

      CsvToAvroApp.main(Array(
        "--sourceDir", in,
        "--destDir",   out,
        "--delimiter", ",",
        "--dedupKey",  "id",
        "--partitionCol", "processing_timestamp"
      ))

      val avro = spark.read.format("avro").load(outPath)
      assert(avro.count() === 2)
      assert(avro.filter($"price".isNull).count() === 1)
    }
  }
}