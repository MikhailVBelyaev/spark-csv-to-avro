# Spark CSV to Avro Converter

A **Docker-ised Apache Spark job** (Scala 2.12 + Spark 3.5) that:

1. Reads **CSV** files from `data/input`
2. **Casts** columns according to a **schema-mapping** (HOCON)
3. **Validates** data (nulls in dedup key → filtered, malformed rows → null)
4. **Deduplicates** by a configurable key
5. Adds a **processing-timestamp** column
6. Writes **Avro** files (partitioned by the timestamp) to `data/output`

---

## Tech Stack

| Component                | Version |
|--------------------------|---------|
| Scala                    | 2.12.18 |
| Apache Spark             | 3.5.0   |
| sbt                      | 1.6.2   |
| Docker + Docker-Compose  | latest  |
| Config                   | Typesafe Config (HOCON) |
| Logging                  | Log4j2  |
| CLI parsing              | scopt 4.1.0 |

---

## Project Layout
spark-csv-to-avro/
├── data/
│   ├── input/          # Put your CSV files here
│   ├── output/         # Avro files appear here
│   └── scripts/        # Generated helper scripts (check_avro.scala)
├── src/
│   ├── main/
│   │   └── scala/com/example/CsvToAvroApp.scala
│   └── test/
│       └── scala/com/example/CsvToAvroAppTest.scala
├── Dockerfile
├── docker-compose.yml
├── build.sbt
├── check_avro_data.sh
├── init_data_dir.sh
└── README.md


---

## Prerequisites

- Docker & Docker-Compose (any recent version)
- `sudo` rights for the one-time permission fix

---

## One-time Setup

```bash
# 1. Fix permissions so the Spark container (uid 185) can write to data/output
bash init_data_dir.sh

# 2. (Optional) Put sample CSV into data/input
cp data/input/sample.csv data/input/

How to Run Locally

# Build images & run the job
docker compose up --build

The job reads all CSV files under data/input.
Output is written to data/output/processing_timestamp=….

Example Usage
1. Full pipeline (default config)
docker compose up --build

Input: data/input/sample.csv
Output: data/output/processing_timestamp=2025-10-28/...

2. Inspect Avro output
bash check_avro_data.sh 2025-10-28

Running Tests

docker compose up spark-test

Generating ScalaDoc

sbt doc

HTML documentation appears in target/scala-2.12/api/.

License
MIT – see LICENSE.