# Datacraft Framework

A framework that eliminates the dependency on Apache Spark by leveraging delta-rs for the creation and management of Delta Lake tables. This framework follows Medallion architecture.

# Libraries

- **Polars** : For reading and writing delta lake tables.
- **SqlModel** : For storing and reading configurations from database tables.
- **Niquests** : For API requests.

# Framework Architecture

## 🟠 Bronze Layer:

Copies data from external sources to a common location.

The framework supports the below external sources out-of-box

- ✅ SFTP Extraction .
- ✅ API Extraction .
- ✅ JDBC/ODBC Extraction .
- ✅ Salesforce/Veeva Extraction .
- ✅ Cloud Object Storage (S3, ADLS, GCP, ...) Extraction .

Copies data from external sources to a common location.

## ⚪ Silver Layer:

Performs Data Standardization and Data Quality Management Checks

**Data Standardization Functions:**

- ✅ Padding.
- ✅ trim.
- ✅ blank_conversion.
- ✅ replace (regex).
- ✅ type_conversion (to lower or upper).
- ✅ sub_string

**Data Quality Management Checks**

- ✅ null checks.
- ✅ unique checks.
- ✅ decimal checks.
- ✅ integer checks.
- ✅ length checks.
- ✅ date checks.
- ✅ domain checks.
- ✅ custom checks.

## 🟡 Gold Layer:

Here transformation logics are performed.

Here SCD-Type 2 is performed and Historical records are maintained in Compute and Only active records are maintained in Publish locations.
