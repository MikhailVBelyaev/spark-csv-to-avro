import org.apache.spark.sql.SparkSession

val spark = SparkSession.builder.getOrCreate()
val df = spark.read.format("avro").load("data/output/processing_timestamp=2025-10-25 09%3A36%3A00.054552")
println("\n===== DATA PREVIEW =====")
df.show(false)
println("\n===== SCHEMA =====")
df.printSchema()
sys.exit(0)
