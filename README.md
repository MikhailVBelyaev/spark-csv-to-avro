# 🚗 Spark CSV → Avro Converter

A **Dockerized Apache Spark job** (Scala 2.12, Spark 3.5) that reads CSV files, validates data, casts columns, removes duplicates, and writes the processed data in **Avro format**.

## 🧱 Tech Stack
- **Language:** Scala 2.12  
- **Framework:** Apache Spark 3.5  
- **Input:** CSV  
- **Output:** Avro  
- **Configuration:** HOCON (`application.conf`)  
- **Build Tool:** sbt  
- **Containerization:** Docker + Docker Compose  

---

## 📁 Project Structure

```
spark-csv-to-avro/
├── data/
│   ├── input/                      # Input CSV files
│   ├── output/                     # Generated Avro files
│   └── scripts/                    # Spark Scala scripts (e.g. check_avro.scala)
├── src/                            # Scala source code
├── Dockerfile
├── docker-compose.yml
├── build.sbt
├── check_avro_data.sh              # Script to inspect Avro data
└── README.md
```

---

## 👤 Spark User and File Permissions

The Spark container runs under **user ID `185` (spark)** for security and reproducibility.  
To ensure proper read/write access, make sure the `data/` directory is owned by your local user.

Run this once before starting the project:

```bash
sudo chown -R $USER:$USER data
chmod -R 775 data
```

This ensures both your local user and the containerized Spark process can read/write files in `data/input` and `data/output`.

---

## ▶️ Run Spark Job

To build and start the Spark job:

```bash
docker compose up --build
```

This command:
- Builds the Spark image
- Mounts the `data/` directory
- Executes the Scala Spark job that processes CSV → Avro

---

## 🔍 Check Avro Data

You can inspect an Avro output file directly using the helper script:

```bash
bash check_avro_data.sh 2025-10-24
```

This will:
- Generate a temporary Scala script (`data/scripts/check_avro.scala`)
- Launch a Spark container with `spark-shell`
- Load the Avro file from `data/output/processing_date=2025-10-24`
- Display the schema and sample data

Example output:

```
===== DATA PREVIEW =====
+---+-----+-------+------------+
|id |name |price  |created_date|
+---+-----+-------+------------+
|1  |Car A|12000.5|2025-10-20  |
|2  |Car B|13500.0|2025-10-21  |
|3  |Car C|14000.0|2025-10-22  |
+---+-----+-------+------------+

===== SCHEMA =====
root
 |-- id: integer (nullable = true)
 |-- name: string (nullable = true)
 |-- price: double (nullable = true)
 |-- created_date: date (nullable = true)
```

---

## 🧩 Notes
- All Spark code runs inside the container — no Spark installation required locally.
- Data persistence between runs is handled through the `data/` volume.
- You can freely modify and re-run the CSV → Avro conversion pipeline.

---