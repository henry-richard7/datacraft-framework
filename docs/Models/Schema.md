# Documentation for `Schema`

Here you can find the documentation for Datacraft Framework's Configuration Tables.

::: datacraft_framework.Models.schema.ctlDataAcquisitionConnectionMaster

??? Fields

    | Field Name                 | Type            | Description                                                      | Primary Key                       |
    | -------------------------- | --------------- | ---------------------------------------------------------------- | --------------------------------- |
    | `outbound_source_platform` | `str`           | The platform in which the source file is stored.                 | <span style='color:red'>\*</span> |
    | `outbound_source_system`   | `str`           | The unique identifier for the source credentials.                | <span style='color:red'>\*</span> |
    | `connection_config`        | `Optional[str]` | JSON containing the connection details like hostname, port, etc. |                                   |
    | `ssh_private_key`          | `Optional[str]` | Private key file contents for SFTP connection authentication.    |                                   |

::: datacraft_framework.Models.schema.ctlApiConnectionsDtl

??? Fields

    | Field Name                 | Type            | Description                                                                   | Primary Key                       |
    | -------------------------- | --------------- | ----------------------------------------------------------------------------- | --------------------------------- |
    | `seq_no`                   | `int`           | Auto-incremented sequence number.                                             | <span style='color:red'>\*</span> |
    | `pre_ingestion_dataset_id` | `Optional[int]` | The dataset ID for RAW/Bronze layer associated with this API call.            |                                   |
    | `outbound_source_system`   | `Optional[str]` | The unique identifier for the source credentials used for the API.            |                                   |
    | `type`                     | `Optional[str]` | The API request type. Expected values: ['TOKEN', 'RESPONSE', 'CUSTOM']        |                                   |
    | `token_url`                | `Optional[str]` | URL to fetch a token if type == 'TOKEN'.                                      |                                   |
    | `auth_type`                | `Optional[str]` | Type of authentication used. Supported types: OAuth, JWT, Basic Auth, CUSTOM. |                                   |
    | `token_type`               | `Optional[str]` | Type of token used (e.g., Bearer).                                            |                                   |
    | `client_id`                | `Optional[str]` | Client ID used to request a token.                                            |                                   |
    | `client_secret`            | `Optional[str]` | Client secret used to request a token.                                        |                                   |
    | `username`                 | `Optional[str]` | Username used for basic authentication.                                       |                                   |
    | `password`                 | `Optional[str]` | Password used for basic authentication.                                       |                                   |
    | `issuer`                   | `Optional[str]` | Issuer required for JWT or service account token requests.                    |                                   |
    | `scope`                    | `Optional[str]` | Scope required for JWT or service account token requests.                     |                                   |
    | `private_key`              | `Optional[str]` | Private key required for JWT or service account token requests.               |                                   |
    | `token_path`               | `Optional[str]` | JSON path where the token exists in the response body.                        |                                   |
    | `method`                   | `Optional[str]` | HTTP method used for the API request (e.g., GET, POST).                       |                                   |
    | `url`                      | `Optional[str]` | API endpoint URL to send the request to.                                      |                                   |
    | `headers`                  | `Optional[str]` | HTTP headers to be sent with the request as JSON string.                      |                                   |
    | `params`                   | `Optional[str]` | Query parameters to be appended to the request URL.                           |                                   |
    | `data`                     | `Optional[str]` | Raw body content sent when making an API request.                             |                                   |
    | `json_body`                | `Optional[str]` | JSON-formatted body sent when making an API request.                          |                                   |
    | `body_values`              | `Optional[str]` | Placeholders in the request body that need dynamic replacement.               |                                   |

::: datacraft_framework.Models.schema.ctlDataAcquisitionDetail

??? Fields

    | Field Name                            | Type            | Description                                                                                             | Primary Key                       |
    | ------------------------------------- | --------------- | ------------------------------------------------------------------------------------------------------- | --------------------------------- |
    | `process_id`                          | `int`           | The unique ID representing a specific data acquisition process.                                         | <span style='color:red'>\*</span> |
    | `pre_ingestion_dataset_id`            | `int`           | The Bronze Layer dataset ID. Use Salesforce object name if source platform is Salesforce.               | <span style='color:red'>\*</span> |
    | `pre_ingestion_dataset_name`          | `Optional[str]` | The Bronze Layer dataset name.                                                                          |                                   |
    | `outbound_source_platform`            | `Optional[str]` | Platform where the source file is stored (e.g., SFTP, DB, Salesforce).                                  |                                   |
    | `outbound_source_system`              | `Optional[str]` | Unique identifier for the source system credentials.                                                    |                                   |
    | `outbound_source_location`            | `Optional[str]` | Location where the source file is stored (e.g., directory path or database schema).                     |                                   |
    | `outbound_source_file_pattern_static` | `Optional[str]` | Flag indicating whether the source filename is static ('Y' or 'N').                                     |                                   |
    | `outbound_source_file_pattern`        | `Optional[str]` | File name pattern of the source file. Supports YYYY-MM-DD and regex patterns.                           |                                   |
    | `outbound_source_file_format`         | `Optional[str]` | Format of the source file (e.g., CSV, JSON, XML).                                                       |                                   |
    | `outbound_file_delimiter`             | `Optional[str]` | Delimiter used in the source file (e.g., comma, tab).                                                   |                                   |
    | `query`                               | `Optional[str]` | SQL query to fetch data from a database. Use only when source platform is a database.                   |                                   |
    | `columns`                             | `Optional[str]` | Comma-separated list of columns to select from Salesforce. Use only when source platform is Salesforce. |                                   |
    | `inbound_location`                    | `Optional[str]` | Destination location where the acquired data will be saved.                                             |                                   |

::: datacraft_framework.Models.schema.logDataAcquisitionDetail

??? Fields

    | Field Name                 | Type                 | Description                                              | Primary Key                       |
    | -------------------------- | -------------------- | -------------------------------------------------------- | --------------------------------- |
    | `seq_no`                   | `int`                | Auto-incremented sequence number.                        | <span style='color:red'>\*</span> |
    | `batch_id`                 | `Optional[int]`      | Batch ID of the running process.                         |                                   |
    | `run_date`                 | `Optional[date]`     | Date on which the process was executed.                  |                                   |
    | `process_id`               | `Optional[int]`      | Unique ID of the process.                                |                                   |
    | `pre_ingestion_dataset_id` | `Optional[int]`      | Dataset ID of the Bronze Layer.                          |                                   |
    | `outbound_source_location` | `Optional[str]`      | Source file location.                                    |                                   |
    | `inbound_file_location`    | `Optional[str]`      | Inbound file storage location.                           |                                   |
    | `status`                   | `Optional[str]`      | Current status of the BRONZE dataset process.            |                                   |
    | `exception_details`        | `Optional[str]`      | Details of any exceptions encountered during processing. |                                   |
    | `start_time`               | `Optional[datetime]` | Start time of the process.                               |                                   |
    | `end_time`                 | `Optional[datetime]` | End time of the process.                                 |                                   |

::: datacraft_framework.Models.schema.logRawProcessDtl

??? Fields

    | Field Name                | Type                 | Description                                       | Primary Key                       |
    | ------------------------- | -------------------- | ------------------------------------------------- | --------------------------------- |
    | `file_id`                 | `Optional[int]`      | Unique identifier for the processed file.         | <span style='color:red'>\*</span> |
    | `run_date`                | `Optional[date]`     | Date when the file was processed.                 |                                   |
    | `batch_id`                | `Optional[int]`      | Batch ID associated with the file processing.     |                                   |
    | `process_id`              | `Optional[int]`      | ID of the process responsible for file ingestion. |                                   |
    | `dataset_id`              | `Optional[int]`      | ID of the dataset being processed.                |                                   |
    | `source_file`             | `Optional[str]`      | Name of the source file being processed.          |                                   |
    | `landing_location`        | `Optional[str]`      | Path where the file was saved in Landing Layer.   |                                   |
    | `file_status`             | `Optional[str]`      | Status of the file processing.                    |                                   |
    | `exception_details`       | `Optional[str]`      | Error message if file processing failed.          |                                   |
    | `file_process_start_time` | `Optional[datetime]` | Start time of file processing.                    |                                   |
    | `file_process_end_time`   | `Optional[datetime]` | End time of file processing.                      |                                   |

::: datacraft_framework.Models.schema.ctlDatasetMaster

??? Fields

    | Field Name                               | Type            | Description                                                              | Primary Key                       |
    | ---------------------------------------- | --------------- | ------------------------------------------------------------------------ | --------------------------------- |
    | `process_id`                             | `int`           | Unique identifier for the data pipeline process.                         | <span style='color:red'>\*</span> |
    | `dataset_id`                             | `int`           | Unique identifier for the dataset.                                       | <span style='color:red'>\*</span> |
    | `dataset_name`                           | `Optional[str]` | Name of the dataset.                                                     |                                   |
    | `dataset_type`                           | `Optional[str]` | Type of dataset (e.g., RAW, Bronze, Silver, Gold).                       |                                   |
    | `provider`                               | `Optional[str]` | Source system or provider of the dataset.                                |                                   |
    | `subject_area`                           | `Optional[str]` | Business domain or subject area this dataset belongs to.                 |                                   |
    | `inbound_location`                       | `Optional[str]` | File system path where raw data is initially received.                   |                                   |
    | `inbound_file_pattern`                   | `Optional[str]` | Pattern used to match incoming files (supports date patterns and regex). |                                   |
    | `inbound_static_file_pattern`            | `Optional[str]` | Flag indicating if the inbound filename is static ('Y'/'N').             |                                   |
    | `inbound_file_format`                    | `Optional[str]` | Format of the source file (e.g., CSV, JSON, XML).                        |                                   |
    | `inbound_file_delimiter`                 | `Optional[str]` | Delimiter used in flat files (e.g., comma, tab).                         |                                   |
    | `header_available_flag`                  | `Optional[str]` | Indicates whether the file has a header row ('Y'/'N').                   |                                   |
    | `file_number_of_fields`                  | `Optional[int]` | Expected number of columns in the source file.                           |                                   |
    | `landing_location`                       | `Optional[str]` | Path where data is stored after landing (RAW/Landing Layer).             |                                   |
    | `landing_table`                          | `Optional[str]` | Target table name for structured storage in the Landing Layer.           |                                   |
    | `landing_partition_columns`              | `Optional[str]` | Partitioning strategy for the Landing Layer table.                       |                                   |
    | `data_standardisation_location`          | `Optional[str]` | Path where standardized version of the dataset is stored.                |                                   |
    | `data_standardisation_table`             | `Optional[str]` | Table name for the standardized dataset.                                 |                                   |
    | `data_standardisation_partition_columns` | `Optional[str]` | Partitioning strategy for the Standardized Layer.                        |                                   |
    | `dqm_location`                           | `Optional[str]` | Location where data quality checked version is stored.                   |                                   |
    | `dqm_table`                              | `Optional[str]` | Table name for the Data Quality Management (DQM) layer.                  |                                   |
    | `dqm_partition_columns`                  | `Optional[str]` | Partitioning strategy for the DQM Layer.                                 |                                   |
    | `staging_location`                       | `Optional[str]` | Path where data is stored before transformation into final format.       |                                   |
    | `staging_table`                          | `Optional[str]` | Table name for the Staging Layer.                                        |                                   |
    | `staging_partition_columns`              | `Optional[str]` | Partitioning strategy for the Staging Layer.                             |                                   |
    | `transformation_location`                | `Optional[str]` | Path where transformed dataset is stored.                                |                                   |
    | `transformation_table`                   | `Optional[str]` | Table name for the Transformation Layer.                                 |                                   |
    | `transformation_partition_columns`       | `Optional[str]` | Partitioning strategy for the Transformation Layer.                      |                                   |
    | `archive_location`                       | `Optional[str]` | Archive path for historical versions of the dataset.                     |                                   |
    | `publish_location`                       | `Optional[str]` | Final destination where the dataset is published for consumption.        |                                   |
    | `publish_table`                          | `Optional[str]` | Table name for the Published Layer.                                      |                                   |
    | `publish_partition_columns`              | `Optional[str]` | Partitioning strategy for the Published Layer.                           |                                   |

::: datacraft_framework.Models.schema.CtlColumnMetadata

??? Fields

    | Field Name               | Type            | Description                                                           | Primary Key                       |
    | ------------------------ | --------------- | --------------------------------------------------------------------- | --------------------------------- |
    | `column_id`              | `Optional[int]` | Unique identifier for the column.                                     | <span style='color:red'>\*</span> |
    | `table_name`             | `str`           | Name of the table for which the column metadata is defined.           | <span style='color:red'>\*</span> |
    | `dataset_id`             | `int`           | ID of the dataset this column belongs to.                             | <span style='color:red'>\*</span> |
    | `column_name`            | `str`           | Name of the column.                                                   | <span style='color:red'>\*</span> |
    | `column_data_type`       | `Optional[str]` | Data type of the column (e.g., string, integer, float, date).         |                                   |
    | `column_date_format`     | `Optional[str]` | Date format if column_data_type is Date (e.g., 'yyyy-MM-dd').         |                                   |
    | `column_description`     | `Optional[str]` | Description explaining the meaning or business context of the column. |                                   |
    | `column_json_mapping`    | `Optional[str]` | JSON path mapping used to extract value from raw JSON input.          |                                   |
    | `source_column_name`     | `Optional[str]` | Original column name from the source dataset.                         |                                   |
    | `column_sequence_number` | `Optional[int]` | Order of the column within the table structure.                       |                                   |
    | `column_tag`             | `Optional[str]` | Tags for column usage in dashboards (e.g., KPI, KPI-Filter).          |                                   |

::: datacraft_framework.Models.schema.ctlDataStandardisationDtl

??? Fields

    | Field Name        | Type            | Description                                                                            | Primary Key                       |
    | ----------------- | --------------- | -------------------------------------------------------------------------------------- | --------------------------------- |
    | `dataset_id`      | `int`           | ID of the dataset being standardized.                                                  | <span style='color:red'>\*</span> |
    | `column_name`     | `str`           | Name of the column to which the standardization rule applies.                          | <span style='color:red'>\*</span> |
    | `function_name`   | `Optional[str]` | Name of the function used for standardizing the column (e.g., trim, cast, parse_date). |                                   |
    | `function_params` | `Optional[str]` | Parameters passed to the function (as JSON or string).                                 |                                   |

::: datacraft_framework.Models.schema.ctlDqmMasterDtl

??? Fields

    | Field Name                  | Type            | Description                                                              | Primary Key                       |
    | --------------------------- | --------------- | ------------------------------------------------------------------------ | --------------------------------- |
    | `qc_id`                     | `int`           | Unique ID for the data quality check.                                    | <span style='color:red'>\*</span> |
    | `process_id`                | `Optional[int]` | ID of the process associated with the QC rule.                           |                                   |
    | `dataset_id`                | `Optional[int]` | ID of the dataset being validated.                                       |                                   |
    | `column_name`               | `Optional[str]` | Name of the column being validated.                                      |                                   |
    | `qc_type`                   | `Optional[str]` | Type of quality check (e.g., not_null, unique, range, regex).            |                                   |
    | `qc_param`                  | `Optional[str]` | Parameters for the QC rule (e.g., min/max values, regex pattern).        |                                   |
    | `active_flag`               | `Optional[str]` | Whether the QC rule is active ('Y'/'N').                                 |                                   |
    | `qc_filter`                 | `Optional[str]` | Optional filter condition for applying the QC rule.                      |                                   |
    | `criticality`               | `Optional[str]` | Severity level of the QC failure (e.g., HIGH, MEDIUM, LOW).              |                                   |
    | `criticality_threshold_pct` | `Optional[int]` | Threshold percentage for criticality violation (e.g., 5% nulls allowed). |                                   |

::: datacraft_framework.Models.schema.logDataStandardisationDtl

??? Fields

    | Field Name                      | Type                 | Description                                                            | Primary Key                       |
    | ------------------------------- | -------------------- | ---------------------------------------------------------------------- | --------------------------------- |
    | `seq_no`                        | `Optional[int]`      | Auto-incremented sequence number for logging entries.                  | <span style='color:red'>\*</span> |
    | `batch_id`                      | `Optional[int]`      | Batch ID associated with the standardization process.                  |                                   |
    | `process_id`                    | `Optional[int]`      | ID of the data pipeline process involved.                              |                                   |
    | `dataset_id`                    | `Optional[int]`      | ID of the dataset undergoing standardization.                          |                                   |
    | `source_file`                   | `Optional[str]`      | Name of the source file processed during standardization.              |                                   |
    | `data_standardisation_location` | `Optional[str]`      | Path where standardized output was written.                            |                                   |
    | `status`                        | `Optional[str]`      | Current status of the standardization process (e.g., SUCCESS, FAILED). |                                   |
    | `exception_details`             | `Optional[str]`      | Details of any exceptions encountered during processing.               |                                   |
    | `start_datetime`                | `Optional[datetime]` | Start time of the standardization process.                             |                                   |
    | `end_datetime`                  | `Optional[datetime]` | End time of the standardization process.                               |                                   |

::: datacraft_framework.Models.schema.logDqmDtl

??? Fields

    | Field Name                  | Type                 | Description                                                  | Primary Key                       |
    | --------------------------- | -------------------- | ------------------------------------------------------------ | --------------------------------- |
    | `seq_no`                    | `Optional[int]`      | Auto-incremented sequence number for logging entries.        | <span style='color:red'>\*</span> |
    | `process_id`                | `Optional[int]`      | ID of the data pipeline process involved.                    |                                   |
    | `dataset_id`                | `Optional[int]`      | ID of the dataset being validated.                           |                                   |
    | `batch_id`                  | `Optional[int]`      | Batch ID associated with the DQM run.                        |                                   |
    | `source_file`               | `Optional[str]`      | Name of the source file processed during DQM validation.     |                                   |
    | `column_name`               | `Optional[str]`      | Name of the column being validated.                          |                                   |
    | `qc_type`                   | `Optional[str]`      | Type of quality check performed (e.g., not_null, unique).    |                                   |
    | `qc_param`                  | `Optional[str]`      | Parameters used for the QC rule (e.g., regex, value ranges). |                                   |
    | `qc_filter`                 | `Optional[str]`      | Filter condition applied during QC validation.               |                                   |
    | `criticality`               | `Optional[str]`      | Severity level of the QC failure (e.g., HIGH, MEDIUM).       |                                   |
    | `criticality_threshold_pct` | `Optional[int]`      | Threshold percentage for criticality violation.              |                                   |
    | `error_count`               | `Optional[int]`      | Number of records failing the QC rule.                       |                                   |
    | `error_pct`                 | `Optional[int]`      | Percentage of records failing the QC rule.                   |                                   |
    | `status`                    | `Optional[str]`      | Status of the QC validation (e.g., PASSED, FAILED).          |                                   |
    | `dqm_start_time`            | `Optional[datetime]` | Start time of the DQM validation process.                    |                                   |
    | `dqm_end_time`              | `Optional[datetime]` | End time of the DQM validation process.                      |                                   |

::: datacraft_framework.Models.schema.ctlTransformationDependencyMaster

??? Fields

    | Field Name                    | Type            | Description                                                        | Primary Key                       |
    | ----------------------------- | --------------- | ------------------------------------------------------------------ | --------------------------------- |
    | `process_id`                  | `int`           | ID of the transformation process.                                  | <span style='color:red'>\*</span> |
    | `transformation_step`         | `Optional[str]` | Order of transformation step (e.g., 'JOIN', 'AGGREGATE').          |                                   |
    | `dataset_id`                  | `int`           | ID of the current dataset being transformed.                       | <span style='color:red'>\*</span> |
    | `depedent_dataset_id`         | `int`           | ID of the dependent dataset needed for transformation.             | <span style='color:red'>\*</span> |
    | `transformation_type`         | `Optional[str]` | Type of transformation logic (e.g., JOIN, UNION, CUSTOM).          |                                   |
    | `join_how`                    | `Optional[str]` | Join type (e.g., inner, left, outer).                              |                                   |
    | `left_table_columns`          | `Optional[str]` | Columns from the left table involved in the join.                  |                                   |
    | `right_table_columns`         | `Optional[str]` | Columns from the right table involved in the join.                 |                                   |
    | `primary_keys`                | `Optional[str]` | List of primary keys for the resulting transformed dataset.        |                                   |
    | `custom_transformation_query` | `Optional[str]` | Custom SQL or transformation query when using custom logic.        |                                   |
    | `extra_values`                | `Optional[str]` | Additional parameters or metadata required for the transformation. |                                   |

::: datacraft_framework.Models.schema.logTransformationDtl

??? Fields

    | Field Name                  | Type                 | Description                                                       | Primary Key                       |
    | --------------------------- | -------------------- | ----------------------------------------------------------------- | --------------------------------- |
    | `seq_no`                    | `Optional[int]`      | Auto-incremented sequence number for logging entries.             | <span style='color:red'>\*</span> |
    | `batch_id`                  | `Optional[int]`      | Batch ID associated with the transformation process.              |                                   |
    | `data_date`                 | `Optional[date]`     | Date of the data being transformed (typically partitioning key).  |                                   |
    | `process_id`                | `Optional[int]`      | ID of the transformation process.                                 |                                   |
    | `dataset_id`                | `Optional[int]`      | ID of the dataset undergoing transformation.                      |                                   |
    | `source_file`               | `Optional[str]`      | Name of the source file processed during transformation.          |                                   |
    | `status`                    | `Optional[str]`      | Current status of the transformation job (e.g., SUCCESS, FAILED). |                                   |
    | `exception_details`         | `Optional[str]`      | Details of any exceptions encountered during transformation.      |                                   |
    | `transformation_start_time` | `Optional[datetime]` | Start time of the transformation job.                             |                                   |
    | `transformation_end_time`   | `Optional[datetime]` | End time of the transformation job.                               |                                   |
