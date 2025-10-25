# Requirements

## Technical Stack
- **Language**: Scala 2.12 or 2.13
- **Build Tool**: sbt
- **Framework**: Apache Spark 3.x
- **Input Format**: CSV
- **Output Format**: Avro
- **Storage**: Local File System

## Functional Requirements

### 1. Read CSV Files
- Read CSV files from a source directory on the local file system
- Handle CSV headers appropriately
- Support configurable delimiter (default: comma)
- Handle malformed records gracefully

### 2. Data Processing
- Implement basic data validation (null checks, data type validation)
- Add a processing timestamp column to each record
- Remove duplicate records based on a configurable key field
- Handle schema inference from CSV headers

### 3. Schema Management & Type Casting
- Support explicit column type casting to Spark data types
- Allow configuration-based schema definition with column name and target type mapping
- Support the following Spark data types:
  - `StringType`
  - `IntegerType`
  - `LongType`
  - `DoubleType`
  - `FloatType`
  - `BooleanType`
  - `DateType` (with configurable date format, e.g., "yyyy-MM-dd", "dd/MM/yyyy")
  - `TimestampType` (with configurable timestamp format, e.g., "yyyy-MM-dd HH:mm:ss")
  - `DecimalType` (with precision and scale)
- Handle type casting errors gracefully (e.g., log warnings, set to null, or use default values)
- Preserve columns not specified in the type mapping with their inferred types
- For `DateType` and `TimestampType`, allow specifying custom format patterns in the configuration

### 4. Write Avro Files
- Convert processed data to Avro format
- Write output to a destination directory on the local file system
- Implement partitioning strategy (e.g., by date or category)
- Include proper Avro schema definition
- Ensure Avro schema reflects the casted data types

### 5. Configuration
- Use application.conf or command-line arguments for:
  - Source directory path
  - Destination directory path
  - Deduplication key field name
  - Partition column name (optional)
  - **Schema mapping configuration** (column name → data type)
  - **Date format patterns** (for DateType columns, e.g., "yyyy-MM-dd")
  - **Timestamp format patterns** (for TimestampType columns, e.g., "yyyy-MM-dd HH:mm:ss")

### 6. Error Handling & Logging
- Implement proper error handling
- Add structured logging (using a logging framework like Log4j or Logback)
- Log processing statistics (records read, written, failed)
- Log type casting failures with details

## Non-Functional Requirements

### 1. Code Quality
- Follow Scala best practices and coding standards
- Write clean, readable, and maintainable code
- Use meaningful variable and function names
- Add appropriate comments for complex logic

### 2. Testing
- Write unit tests using ScalaTest or Specs2
- Achieve at least 70% code coverage
- Include integration tests (optional but preferred)
- **Test type casting functionality with various data types**
- **Test date and timestamp parsing with different format patterns**

### 3. Build Configuration
- Proper sbt build configuration
- Include all necessary dependencies
- Support for creating a fat JAR for deployment

### 4. Documentation
- README with:
  - Project description
  - Setup instructions
  - How to run locally
  - Configuration parameters
  - **Schema mapping configuration examples**
  - **Date and timestamp format examples**
  - Example usage
- Code documentation (ScalaDoc)