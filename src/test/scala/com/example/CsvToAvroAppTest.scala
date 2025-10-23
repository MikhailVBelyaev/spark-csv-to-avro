package com.example

import org.scalatest.funsuite.AnyFunSuite
import org.apache.spark.sql.SparkSession
import com.typesafe.config.ConfigFactory

class CsvToAvroAppTest extends AnyFunSuite {

  val spark = SparkSession.builder()
    .master("local[*]")
    .appName("TestApp")
    .getOrCreate()

  import spark.implicits._

  test("Integer casting works") {
    val df = Seq(("1", "2")).toDF("id", "price")

    val confStr =
      """
        |app {
        | schemaMapping {
        |   id = "IntegerType"
        |   price = "DoubleType"
        | }
        |}
        |""".stripMargin

    val conf = ConfigFactory.parseString(confStr).getConfig("app.schemaMapping")
    val (res, _) = CsvToAvroApp.safeCastColumns(df, conf)
    assert(res.columns.contains("id"))
    assert(res.first().getAs[Int]("id") == 1)
  }
}