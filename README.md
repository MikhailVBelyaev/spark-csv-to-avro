Spark CSV to Avro Converter

A Dockerized Apache Spark job (Scala 2.12 + Spark 3.5) that performs end-to-end data transformation:
	1.	Reads CSV files from data/input
	2.	Casts columns according to schema mapping (defined in HOCON configuration)
	3.	Validates and filters invalid or malformed records
	4.	Deduplicates records based on a configurable key
	5.	Adds a processing timestamp column
	6.	Writes Avro files partitioned by processing timestamp into data/output

⸻

1. Overview

This project demonstrates a robust and configurable ETL pipeline built on Apache Spark, packaged in Docker for reproducibility.
It is designed for converting heterogeneous CSV datasets into validated, typed Avro outputs with consistent schema enforcement and timestamp partitioning.

⸻

2. Technology Stack

| Component                | Version |
|--------------------------|---------|
| Scala                    | 2.12.18 |
| Apache Spark             | 3.5.0   |
| sbt                      | 1.6.2   |
| Docker + Docker-Compose  | latest  |
| Config                   | Typesafe Config (HOCON) |
| Logging                  | Log4j2  |
| CLI parsing              | scopt 4.1.0 |

⸻

3. Project Structure

spark-csv-to-avro/
├── data/
│   ├── input/            # Input CSV files
│   ├── output/           # Output Avro files
│   └── scripts/          # Helper scripts (e.g., check_avro.scala)
├── src/
│   ├── main/
│   │   └── scala/com/example/CsvToAvroApp.scala
│   └── test/
│       └── scala/com/example/CsvToAvroAppTest.scala
├── Dockerfile
├── docker-compose.yml
├── build.sbt
├── application.conf
├── check_avro_data.sh
├── init_data_dir.sh
└── README.md

⸻

4. Configuration

The main configuration file is located at src/main/resources/application.conf:

app {
sourceDir = “data/input”
destDir = “data/output”
delimiter = “,”
dedupKey = “id”
partitionColumn = “processing_timestamp”

schemaMapping {
id = “IntegerType”
name = “StringType”
price = “DoubleType”
age = “LongType”
height = “FloatType”
is_active = “BooleanType”
created_date = “DateType:yyyy-MM-dd”
updated_at = “TimestampType:yyyy-MM-dd HH:mm:ss”
balance = “DecimalType:10,2”
}

dateFormat = “yyyy-MM-dd”
timestampFormat = “yyyy-MM-dd HH:mm:ss”
}

4.1 Parameters

Parameter	Description
sourceDir	Directory containing input CSV files
destDir	Output directory for Avro files
delimiter	CSV delimiter (default: ,)
dedupKey	Column used for deduplication
partitionColumn	Timestamp column added during processing
schemaMapping	Type mapping for each column
dateFormat	Default date format
timestampFormat	Default timestamp format

4.2 Schema Mapping Examples

schemaMapping {
id = “IntegerType”
model = “StringType”
year = “IntegerType”
price = “DoubleType”
}

4.3 Date and Timestamp Format Examples

Type	Example Input	Parsed Result
DateType:yyyy-MM-dd	2023-01-01	2023-01-01
DateType:dd/MM/yyyy	01/02/2023	2023-02-01
TimestampType:yyyy-MM-dd HH:mm:ss	2023-01-01 10:30:00	2023-01-01 10:30:00
TimestampType:dd-MM-yyyy HH:mm:ss	01-02-2023 12:00:00	2023-02-01 12:00:00


⸻

5. Setup Instructions

5.1 Prerequisites
	•	Docker and Docker Compose installed
	•	sudo rights for directory permission setup

5.2 Initialize Data Directories

bash init_data_dir.sh

This script creates input/output directories and assigns correct permissions for the Spark container (UID 185).

5.3 Add Sample CSV (Optional)

A sample dataset is provided at data/input/sample.csv for testing.

⸻

6. Running the Project

6.1 Build and Run the Job

docker compose up --build

This command:
	•	Builds Docker images for both app and test stages
	•	Reads CSV files from data/input
	•	Performs type casting, validation, and deduplication
	•	Writes Avro outputs to data/output/processing_timestamp=...

6.2 Run Only the Spark App

docker compose up spark-app

6.3 Run Tests

docker compose up spark-test

Runs all ScalaTest suites defined in CsvToAvroAppTest.scala.

⸻

7. Inspecting Avro Output

Use the helper script to open Avro files inside Spark Shell:

bash check_avro_data.sh 2025-10-28

This will:
	1.	Locate the Avro folder for the given date
	2.	Generate a Scala script under data/scripts/check_avro.scala
	3.	Open spark-shell inside the container
	4.	Display schema and a data preview

⸻

8. Example Workflow

Input (data/input/sample.csv)

id,name,price,age,height,is_active,created_date,updated_at,balance
1,Alice,99.99,25,1.65,true,2023-01-01,2023-01-01 10:30:00,123.45
2,Bob,invalid,30,1.75,false,2023-01-02,2023-01-02 12:00:00,456.78
3,Charlie,150.50,,1.80,true,invalid,2023-01-03 14:15:00,789.12

Output (data/output/processing_timestamp=…)

+—+––––+——+—+——+–––––+————+—————––+––––+
|id |name    |price |age|height|is_active |created_date|updated_at         |balance |
+—+––––+——+—+——+–––––+————+—————––+––––+
|1  |Alice   |99.99 |25 |1.65  |true      |2023-01-01  |2023-01-01 10:30:00|123.45  |
|2  |Bob     |null  |30 |1.75  |false     |2023-01-02  |2023-01-02 12:00:00|456.78  |
|3  |Charlie |150.5 |null|1.8  |true      |null        |2023-01-03 14:15:00|789.12  |
+—+––––+——+—+——+–––––+————+—————––+––––+

⸻

9. Command Reference

Action	Command
Build and run pipeline	docker compose up --build
Run only Spark app	docker compose up spark-app
Run unit tests	docker compose up spark-test
Inspect Avro data	bash check_avro_data.sh <DATE>
Initialize folders	bash init_data_dir.sh
Generate ScalaDoc	sbt doc


⸻

10. Code Documentation (ScalaDoc)

To generate API documentation:

sbt doc

The output will be available in:

target/scala-2.12/api/

⸻

11. Design Notes
	•	The Spark job uses a configurable schema for type casting and validation.
	•	Invalid records are logged and converted to null values.
	•	Deduplication occurs after validation, based on dedupKey.
	•	Timestamps are automatically adjusted to UTC+5 for Tashkent time zone.
	•	All processing and logging occur inside the container for reproducibility.

⸻

12. License

This project is licensed under the MIT License.
See LICENSE for details.

⸻

13. Author

Mikhail Belyaev
GitHub: https://github.com/MikhailVBelyaev

⸻