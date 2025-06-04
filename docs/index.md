# Welcome to Datacraft Framework

## ℹ️ About

Datacraft Framework is a high-performance, modular data processing framework designed to streamline and standardize dataset onboarding for modern data lakehouses. Built on Polars, a blazing-fast DataFrame library, the framework emphasizes speed, scalability, and clarity.

It follows the Medallion Architecture with distinct Bronze, Silver, and Gold layers:

- **Bronze Layer:** Ingests raw data from diverse sources (e.g., files, APIs, databases) with minimal processing.

- **Silver Layer:** Performs schema alignment, type casting, and robust data quality management (DQM) including null checks, pattern matching, and threshold validations.

- **Gold Layer:** Applies business logic, joins, and advanced transformations to produce refined, analytics-ready datasets.

## ✨ Key Features

- ✅ Schema-driven configuration using sqlmodel control tables for declarative onboarding.

- 🚀 Ultra-fast processing using Polars, optimized for both in-memory and out-of-core execution.

- 📦 Plug-and-play connectors for file systems, SFTP, S3, and SQL-based sources.

- 🛡 Built-in DQM engine for enforcing quality rules at the column level.

- 📊 Data lineage tracking and logging using structured audit tables.

- 🔁 Parallel-safe transformations suitable for large-scale distributed workflows.

## 🧩 Ideal Use Cases

- Fast prototyping and standardization of new data sources
