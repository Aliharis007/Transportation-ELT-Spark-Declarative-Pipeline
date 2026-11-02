# 🚀 Transportation Analytics Pipeline | Databricks Lakeflow Declarative Pipelines

A scalable end-to-end data engineering project built using **Databricks, PySpark, SQL, and Lakeflow Declarative Pipelines**, implementing a Medallion Architecture to ingest, transform, validate, and model transportation data for analytical reporting.

The project demonstrates modern lakehouse engineering practices, including incremental data ingestion with Auto Loader, streaming transformations, Change Data Capture (CDC), Slowly Changing Dimensions (SCD Type 1), data quality expectations, and dimensional modeling.

---

## 📌 Project Overview

This project implements an end-to-end transportation analytics pipeline that processes city and trip datasets through three distinct data layers: **Bronze, Silver, and Gold**.

The pipeline ingests raw CSV datasets containing transportation trip information, standardizes and validates the records, maintains updated trip information through CDC operations, and produces analytical fact tables enriched with geographical and temporal dimensions.

Built using **Databricks Lakeflow Declarative Pipelines**, the implementation leverages a declarative approach to define data transformations and dependencies while allowing Databricks to manage pipeline execution, materialization, and incremental processing.

### Key Objectives

* Implement a Medallion Architecture for structured data processing.
* Ingest transportation datasets using Databricks Auto Loader.
* Enable incremental file ingestion through streaming tables.
* Standardize and validate data across Bronze and Silver layers.
* Implement CDC-based upserts using SCD Type 1 semantics.
* Develop reusable city and calendar dimension tables.
* Build Gold-layer fact tables for transportation analytics.
* Leverage Lakeflow's dependency management and pipeline visualization capabilities.

---

## 🏗️ Architecture

The project follows the **Medallion Architecture**, separating data processing into three layers.

```text
                  SOURCE DATA
                       |
              CSV Files in Unity Catalog
                       |
              +--------+--------+
              |                 |
              ▼                 ▼
        CITY DATA          TRIP DATA
              |                 |
              ▼                 ▼
      +-----------------------------+
      |        BRONZE LAYER         |
      |                             |
      |  City: Materialized View    |
      |  Trips: Streaming Table     |
      |  Auto Loader Ingestion      |
      |  Source Metadata Tracking   |
      +-----------------------------+
              |                 |
              ▼                 ▼
      +-----------------------------+
      |        SILVER LAYER         |
      |                             |
      |  Cleaned City Dimension     |
      |  Calendar Dimension         |
      |  Trips Staging View         |
      |  Data Quality Expectations |
      |  CDC / SCD Type 1 Upserts   |
      +-----------------------------+
                       |
                       ▼
      +-----------------------------+
      |         GOLD LAYER          |
      |                             |
      |  Transportation Fact Table  |
      |  City & Calendar Joins      |
      |  Indore-Specific Fact View  |
      +-----------------------------+
                       |
                       ▼
             ANALYTICAL DATASETS
              BI & REPORTING
```

### Pipeline Lineage

Databricks Lakeflow automatically derives dataset dependencies from the declared transformations and displays them through its pipeline graph.

The graph provides visibility into source ingestion, intermediate transformations, streaming tables, materialized views, and downstream dependencies.

---

## 🥉 1. Bronze Layer — Data Ingestion

The Bronze layer serves as the initial landing and ingestion layer, preserving source-oriented transportation data while capturing ingestion metadata.

Two different ingestion approaches are implemented.

### City Data Ingestion

Implemented using a Databricks materialized view with batch-style CSV ingestion.

**Key Implementations:**

* Read source CSV data directly from Unity Catalog Volumes using PySpark.
* Configured CSV parsing options, including header handling and permissive parsing.
* Captured source file paths using Spark's `_metadata.file_path` column.
* Added ingestion timestamps using `current_timestamp()`.
* Defined Delta table properties for Change Data Feed and file optimization.

**Output:**

`transportation.bronze.city`

### Trips Data Ingestion

Implemented using **Databricks Auto Loader (`cloudFiles`)** with a streaming table.

**Key Implementations:**

* Configured Auto Loader to incrementally discover and ingest incoming CSV files.
* Implemented schema inference and rescue-mode schema evolution.
* Configured micro-batch ingestion using `cloudFiles.maxFilesPerTrigger`.
* Standardized source column naming conventions.
* Captured source file metadata and ingestion timestamps.
* Enabled Delta Change Data Feed and optimization properties.

**Output:**

`transportation.bronze.trips`

---

## 🥈 2. Silver Layer — Data Transformation & Quality

The Silver layer transforms raw Bronze datasets into standardized, structured, and analytics-ready datasets.

It contains three primary components.

### A. City Dimension

Implemented using a materialized view.

**Transformations:**

* Read city records from the Bronze layer.
* Selected relevant business attributes (`city_id`, `city_name`).
* Standardized ingestion metadata by renaming `ingest_datetime` to `bronze_ingest_timestamp`.
* Added `silver_processed_timestamp` for processing traceability.

**Output:**

`transportation.silver.city`

### B. Calendar Dimension

Developed a dynamic calendar dimension using PySpark and Spark SQL.

The calendar is generated using Spark SQL functions such as `sequence()`, `explode()`, and `to_date()`, with configurable date boundaries.

**Calendar Attributes:**

| Attribute          | Description                           |
| ------------------ | ------------------------------------- |
| `date`             | Calendar date                         |
| `date_key`         | Integer-formatted date key (YYYYMMDD) |
| `year`             | Calendar year                         |
| `month`            | Numeric month                         |
| `quarter`          | Calendar quarter                      |
| `day_of_month`     | Day within the month                  |
| `day_of_week`      | Full weekday name                     |
| `day_of_week_abbr` | Abbreviated weekday                   |
| `week_of_year`     | Week number                           |
| `day_of_year`      | Day number within the year            |
| `month_name`       | Month name                            |
| `month_year`       | Month and year reporting label        |
| `quarter_year`     | Quarter and year reporting label      |
| `is_weekday`       | Weekday indicator                     |
| `is_weekend`       | Weekend indicator                     |
| `is_holiday`       | Holiday indicator                     |
| `holiday_name`     | Pakistani holiday description         |

The implementation also incorporates selected Pakistani public holiday dates, including Pakistan Day, Independence Day, Defence Day, and Quaid-e-Azam's Birthday.

**Output:**

`transportation.silver.calendar`

### C. Trips Transformation & CDC

This is the primary transformation component of the Silver layer.

A staging view is created to standardize incoming Bronze trip records before maintaining the final Silver streaming table.

**Data Standardization:**

* Renamed source fields into consistent business-oriented column names.
* Converted trip dates into appropriate date types.
* Standardized passenger categories, distance, fare, and rating attributes.
* Preserved Bronze ingestion timestamps.
* Added Silver processing timestamps.

**Data Quality Expectations:**

Implemented Lakeflow expectations to monitor data quality through predefined validation rules.

| Expectation              | Validation Rule                           |
| ------------------------ | ----------------------------------------- |
| `valid_date`             | Business year must be 2020 or later       |
| `valid_driver_rating`    | Driver rating must be between 1 and 10    |
| `valid_passenger_rating` | Passenger rating must be between 1 and 10 |

**Change Data Capture (CDC):**

Implemented `dp.create_auto_cdc_flow()` to maintain the Silver trips streaming table.

Key configurations:

* **Primary Business Key:** `id` (derived from `trip_id`).
* **Sequence Column:** `silver_processed_timestamp`.
* **SCD Strategy:** Type 1.
* **Update Behavior:** Existing records are updated when matching keys arrive; new records are inserted when no matching key exists.

SCD Type 1 maintains the latest state of each trip record without preserving previous versions.

**Output:**

`transportation.silver.trips`

---

## 🥇 3. Gold Layer — Analytical Data Modeling

The Gold layer produces business-ready analytical datasets by integrating the Silver fact and dimension tables.

### A. Transportation Fact Table

Implemented using a SQL-based materialized view.

**Source Datasets:**

* `transportation.silver.trips`
* `transportation.silver.city`
* `transportation.silver.calendar`

**Key Implementations:**

* Joined transportation trip records with the city dimension using `city_id`.
* Integrated calendar attributes using `business_date`.
* Enriched trip records with city names and temporal reporting attributes.
* Combined transportation measures and descriptive dimensions into a denormalized analytical fact dataset.

**Measures and Attributes:**

* Trip ID
* Business Date
* City ID and City Name
* Passenger Category
* Distance Travelled
* Sales / Fare Amount
* Passenger Rating
* Driver Rating
* Month and Quarter
* Weekday and Weekend Indicators
* Holiday Indicators

**Output:**

`transportation.gold.facts_trips`

### B. City-Specific Analytical View — Indore

Developed an additional materialized view derived from the Gold transportation fact table.

**Implementation:**

* Filtered transportation records using the city identifier `MP01`.
* Created a dedicated analytical dataset for Indore.
* Demonstrated downstream materialized-view dependencies and city-specific data segmentation.

**Output:**

`transportation.gold.fact_trips_indore`

---

## ⚙️ Technologies & Concepts

| Technology                     | Application                                                |
| ------------------------------ | ---------------------------------------------------------- |
| Databricks                     | Lakehouse platform and pipeline execution                  |
| Apache Spark                   | Distributed data processing                                |
| PySpark                        | Data ingestion and transformations                         |
| Spark SQL                      | SQL-based transformations and dimensional modeling         |
| Lakeflow Declarative Pipelines | Declarative pipeline development and dependency management |
| Auto Loader                    | Incremental file ingestion                                 |
| Delta Lake                     | Reliable storage and table management                      |
| Change Data Feed               | Tracking Delta table changes                               |
| CDC                            | Insert and update processing                               |
| SCD Type 1                     | Maintaining the latest record state                        |
| Unity Catalog                  | Data governance and Volume-based source access             |
| Medallion Architecture         | Bronze, Silver, and Gold data organization                 |

### Core Engineering Concepts Demonstrated

* Declarative vs. procedural data pipeline development.
* Batch and streaming data processing.
* Incremental file ingestion.
* Schema inference and schema evolution.
* Data quality expectations.
* Change Data Capture and upsert operations.
* Dimensional modeling and fact-dimension relationships.
* Materialized views and streaming tables.
* Data lineage and dependency management.
* Delta Lake table optimization properties.

---

## 📁 Project Structure

```text
transportation-analytics-pipeline/
│
├── bronze/
│   ├── city.py
│   └── trips.py
│
├── silver/
│   ├── city.py
│   ├── calendar.py
│   └── trips.py
│
├── gold/
│   ├── trips_gold.sql
│   └── trips_indore.sql
│
├── screenshots/
│   └── pipeline_graph.png
│
└── README.md
```

---

## 📊 Pipeline Execution & Visualization

The pipeline is executed through the Databricks Lakeflow Declarative Pipelines interface.

The generated pipeline graph provides a visual representation of:

* Auto Loader source ingestion.
* Bronze streaming table creation.
* Silver staging transformations.
* Materialized-view dependencies.
* CDC-based streaming table updates.
* Gold-layer fact table generation.
* Downstream analytical views.

The graph also provides execution information, including processing duration, output records, and whether a dataset was incrementally updated or fully recomputed.

---

## 🔧 Implementation Approach

Unlike traditional procedural ETL scripts, this project adopts a declarative development approach.

Instead of explicitly managing every table write and transformation sequence, datasets are defined using Lakeflow decorators and APIs, including:

* `@dp.materialized_view`
* `@dp.table`
* `@dp.view`
* `dp.create_streaming_table()`
* `dp.create_auto_cdc_flow()`

These declarations allow Lakeflow to identify dataset dependencies, manage materialization, and coordinate pipeline execution.

This approach improves pipeline organization, maintainability, and visibility into data lineage while reducing the need for manually orchestrated transformation sequences.

---

## 🚀 Potential Extensions

The project provides a foundation for further development, including:

* Integrating additional transportation datasets and cities.
* Implementing SCD Type 2 for historical change tracking.
* Introducing source-based CDC sequencing and advanced deduplication.
* Expanding data quality checks and invalid-record handling.
* Developing transportation performance and revenue dashboards.
* Implementing automated pipeline scheduling and monitoring.
* Optimizing large-scale analytical workloads.

---

## 👨‍💻 Author

**Muhammad Ali Haris**

Computer Science Graduate | Data Engineering | Databricks | PySpark | SQL

* GitHub: [Aliharis007](https://github.com/Aliharis007)
* LinkedIn: [Connect with me](https://www.linkedin.com/)

---

*Built as a hands-on exploration of modern lakehouse engineering, declarative data pipelines, and scalable transportation analytics using Databricks.*
