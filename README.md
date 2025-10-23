# spark-csv-to-avro
# Spark CSV → Avro Converter

A Dockerized Apache Spark job (Scala 2.12, Spark 3.5) that reads CSV files, validates data, casts columns, removes duplicates, and writes Avro output.

## 🧱 Stack
- **Language:** Scala 2.12
- **Framework:** Apache Spark 3.5
- **Input:** CSV
- **Output:** Avro
- **Config:** HOCON (`application.conf`)
- **Build Tool:** sbt
- **Container:** Docker

## ▶️ Run
```bash
docker compose up --build