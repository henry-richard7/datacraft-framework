from sqlmodel import SQLModel, Field
from typing import Optional
from datetime import date, datetime


class ctlDataAcquisitionConnectionMaster(SQLModel, table=True):
    """
    Stores connection details for external data acquisition platforms (e.g., SFTP, databases).
    Used to securely store credentials and configuration for connecting to source systems.
    """

    outbound_source_platform: str = Field(
        primary_key=True, description="The platform in which the source file is stored."
    )
    outbound_source_system: str = Field(
        primary_key=True,
        description="The unique identifier for the source credentials.",
    )
    connection_config: Optional[str] = Field(
        default=None,
        description="JSON containing the connection details like hostname, port, etc.",
    )
    ssh_private_key: Optional[str] = Field(
        default=None,
        description="Private key file contents for SFTP connection authentication.",
    )


class ctlApiConnectionsDtl(SQLModel, table=True):
    """
    Stores detailed API connection configurations including authentication and request parameters.
    """

    seq_no: int = Field(
        primary_key=True, description="Auto-incremented sequence number."
    )
    pre_ingestion_dataset_id: Optional[int] = Field(
        default=None,
        description="The dataset ID for RAW/Bronze layer associated with this API call.",
    )
    outbound_source_system: Optional[str] = Field(
        default=None,
        description="The unique identifier for the source credentials used for the API.",
    )
    type: Optional[str] = Field(
        default=None,
        description="The API request type. Expected values: ['TOKEN', 'RESPONSE', 'CUSTOM']",
    )
    token_url: Optional[str] = Field(
        default=None, description="URL to fetch a token if type == 'TOKEN'."
    )
    auth_type: Optional[str] = Field(
        default=None,
        description="Type of authentication used. Supported types: OAuth, JWT, Basic Auth, CUSTOM.",
    )
    token_type: Optional[str] = Field(
        default=None, description="Type of token used (e.g., Bearer)."
    )
    client_id: Optional[str] = Field(
        default=None, description="Client ID used to request a token."
    )
    client_secret: Optional[str] = Field(
        default=None, description="Client secret used to request a token."
    )
    username: Optional[str] = Field(
        default=None, description="Username used for basic authentication."
    )
    password: Optional[str] = Field(
        default=None, description="Password used for basic authentication."
    )
    issuer: Optional[str] = Field(
        default=None,
        description="Issuer required for JWT or service account token requests.",
    )
    scope: Optional[str] = Field(
        default=None,
        description="Scope required for JWT or service account token requests.",
    )
    private_key: Optional[str] = Field(
        default=None,
        description="Private key required for JWT or service account token requests.",
    )
    token_path: Optional[str] = Field(
        default=None,
        description="JSON path where the token exists in the response body.",
    )
    method: Optional[str] = Field(
        default=None,
        description="HTTP method used for the API request (e.g., GET, POST).",
    )
    url: Optional[str] = Field(
        default=None, description="API endpoint URL to send the request to."
    )
    headers: Optional[str] = Field(
        default=None,
        description="HTTP headers to be sent with the request as JSON string.",
    )
    params: Optional[str] = Field(
        default=None, description="Query parameters to be appended to the request URL."
    )
    data: Optional[str] = Field(
        default=None, description="Raw body content sent when making an API request."
    )
    json_body: Optional[str] = Field(
        default=None, description="JSON-formatted body sent when making an API request."
    )
    body_values: Optional[str] = Field(
        default=None,
        description="Placeholders in the request body that need dynamic replacement.",
    )


class ctlDataAcquisitionDetail(SQLModel, table=True):
    """
    Contains metadata about each data acquisition process such as source location, file format, query, and destination.
    """

    process_id: int = Field(
        primary_key=True,
        description="The unique ID representing a specific data acquisition process.",
    )
    pre_ingestion_dataset_id: int = Field(
        primary_key=True,
        description="The Bronze Layer dataset ID. Use Salesforce object name if source platform is Salesforce.",
    )
    pre_ingestion_dataset_name: Optional[str] = Field(
        default=None, description="The Bronze Layer dataset name."
    )
    outbound_source_platform: Optional[str] = Field(
        default=None,
        description="Platform where the source file is stored (e.g., SFTP, DB, Salesforce).",
    )
    outbound_source_system: Optional[str] = Field(
        default=None, description="Unique identifier for the source system credentials."
    )
    outbound_source_location: Optional[str] = Field(
        default=None,
        description="Location where the source file is stored (e.g., directory path or database schema).",
    )
    outbound_source_file_pattern_static: Optional[str] = Field(
        default=None,
        description="Flag indicating whether the source filename is static ('Y' or 'N').",
    )
    outbound_source_file_pattern: Optional[str] = Field(
        default=None,
        description="File name pattern of the source file. Supports YYYY-MM-DD and regex patterns.",
    )
    outbound_source_file_format: Optional[str] = Field(
        default=None, description="Format of the source file (e.g., CSV, JSON, XML)."
    )
    outbound_file_delimiter: Optional[str] = Field(
        default=None,
        description="Delimiter used in the source file (e.g., comma, tab).",
    )
    query: Optional[str] = Field(
        default=None,
        description="SQL query to fetch data from a database. Use only when source platform is a database.",
    )
    columns: Optional[str] = Field(
        default=None,
        description="Comma-separated list of columns to select from Salesforce. Use only when source platform is Salesforce.",
    )
    inbound_location: Optional[str] = Field(
        default=None,
        description="Destination location where the acquired data will be saved.",
    )


class CtlColumnMetadata(SQLModel, table=True):
    """
    Maintains metadata about individual columns in datasets including data types, descriptions, and mappings.
    """

    column_id: Optional[int] = Field(
        primary_key=True, default=None, description="Unique identifier for the column."
    )
    table_name: str = Field(
        primary_key=True,
        description="Name of the table for which the column metadata is defined.",
    )
    dataset_id: int = Field(
        primary_key=True, description="ID of the dataset this column belongs to."
    )
    column_name: str = Field(primary_key=True, description="Name of the column.")
    column_data_type: Optional[str] = Field(
        default=None,
        description="Data type of the column (e.g., string, integer, float, date).",
    )
    column_date_format: Optional[str] = Field(
        default=None,
        description="Date format if column_data_type is Date (e.g., 'yyyy-MM-dd').",
    )
    column_description: Optional[str] = Field(
        default=None,
        description="Description explaining the meaning or business context of the column.",
    )
    column_json_mapping: Optional[str] = Field(
        default=None,
        description="JSON path mapping used to extract value from raw JSON input.",
    )
    source_column_name: Optional[str] = Field(
        default=None, description="Original column name from the source dataset."
    )
    column_sequence_number: Optional[int] = Field(
        default=None, description="Order of the column within the table structure."
    )
    column_tag: Optional[str] = Field(
        default=None,
        description="Tags for column usage in dashboards (e.g., KPI, KPI-Filter).",
    )


class logDataAcquisitionDetail(SQLModel, table=True):
    """
    Logs execution details of data acquisition processes including status, timing, and exception information.
    """

    seq_no: int = Field(
        primary_key=True, description="Auto-incremented sequence number."
    )
    batch_id: Optional[int] = Field(
        default=None, description="Batch ID of the running process."
    )
    run_date: Optional[date] = Field(
        default=None, description="Date on which the process was executed."
    )
    process_id: Optional[int] = Field(
        default=None, description="Unique ID of the process."
    )
    pre_ingestion_dataset_id: Optional[int] = Field(
        default=None, description="Dataset ID of the Bronze Layer."
    )
    outbound_source_location: Optional[str] = Field(
        default=None, description="Source file location."
    )
    inbound_file_location: Optional[str] = Field(
        default=None, description="Inbound file storage location."
    )
    status: Optional[str] = Field(
        default="IN-PROGRESS",
        description="Current status of the BRONZE dataset process.",
    )
    exception_details: Optional[str] = Field(
        default=None,
        description="Details of any exceptions encountered during processing.",
    )
    start_time: Optional[datetime] = Field(
        default=None, description="Start time of the process."
    )
    end_time: Optional[datetime] = Field(
        default=None, description="End time of the process."
    )


class logRawProcessDtl(SQLModel, table=True):
    """
    Logs processing details for files in the RAW/Landing layer including status and performance metrics.
    """

    file_id: Optional[int] = Field(
        primary_key=True, description="Unique identifier for the processed file."
    )
    run_date: Optional[date] = Field(
        default_factory=date.today, description="Date when the file was processed."
    )
    batch_id: Optional[int] = Field(
        default=None, description="Batch ID associated with the file processing."
    )
    process_id: Optional[int] = Field(
        default=None, description="ID of the process responsible for file ingestion."
    )
    dataset_id: Optional[int] = Field(
        default=None, description="ID of the dataset being processed."
    )
    source_file: Optional[str] = Field(
        default=None, description="Name of the source file being processed."
    )
    landing_location: Optional[str] = Field(
        default=None, description="Path where the file was saved in Landing Layer."
    )
    file_status: Optional[str] = Field(
        default="IN-PROGRESS", description="Status of the file processing."
    )
    exception_details: Optional[str] = Field(
        default=None, description="Error message if file processing failed."
    )
    file_process_start_time: Optional[datetime] = Field(
        default=None, description="Start time of file processing."
    )
    file_process_end_time: Optional[datetime] = Field(
        default=None, description="End time of file processing."
    )


class ctlDatasetMaster(SQLModel, table=True):
    """
    Master configuration table for datasets across all layers (RAW, Landing, Standardization, DQM, Staging, etc.).
    Defines locations, formats, and partitioning strategies for each dataset.
    """

    process_id: int = Field(
        primary_key=True, description="Unique identifier for the data pipeline process."
    )
    dataset_id: int = Field(
        primary_key=True, description="Unique identifier for the dataset."
    )
    dataset_name: Optional[str] = Field(
        default=None, description="Name of the dataset."
    )
    dataset_type: Optional[str] = Field(
        default=None, description="Type of dataset (e.g., RAW, Bronze, Silver, Gold)."
    )
    provider: Optional[str] = Field(
        default=None, description="Source system or provider of the dataset."
    )
    subject_area: Optional[str] = Field(
        default=None,
        description="Business domain or subject area this dataset belongs to.",
    )
    inbound_location: Optional[str] = Field(
        default=None,
        description="File system path where raw data is initially received.",
    )
    inbound_file_pattern: Optional[str] = Field(
        default=None,
        description="Pattern used to match incoming files (supports date patterns and regex).",
    )
    inbound_static_file_pattern: Optional[str] = Field(
        default=None,
        description="Flag indicating if the inbound filename is static ('Y'/'N').",
    )
    inbound_file_format: Optional[str] = Field(
        default=None, description="Format of the source file (e.g., CSV, JSON, XML)."
    )
    inbound_file_delimiter: Optional[str] = Field(
        default=None, description="Delimiter used in flat files (e.g., comma, tab)."
    )
    landing_location: Optional[str] = Field(
        default=None,
        description="Path where data is stored after landing (RAW/Landing Layer).",
    )
    landing_table: Optional[str] = Field(
        default=None,
        description="Target table name for structured storage in the Landing Layer.",
    )
    landing_partition_columns: Optional[str] = Field(
        default=None, description="Partitioning strategy for the Landing Layer table."
    )
    data_standardisation_location: Optional[str] = Field(
        default=None,
        description="Path where standardized version of the dataset is stored.",
    )
    data_standardisation_partition_columns: Optional[str] = Field(
        default=None, description="Partitioning strategy for the Standardized Layer."
    )
    dqm_error_location: Optional[str] = Field(
        default=None,
        description="Location where data quality checked version is stored.",
    )
    dqm_partition_columns: Optional[str] = Field(
        default=None, description="Partitioning strategy for the DQM Layer."
    )
    staging_location: Optional[str] = Field(
        default=None,
        description="Path where data is stored before transformation into final format.",
    )
    staging_table: Optional[str] = Field(
        default=None, description="Table name for the Staging Layer."
    )
    staging_partition_columns: Optional[str] = Field(
        default=None, description="Partitioning strategy for the Staging Layer."
    )
    transformation_location: Optional[str] = Field(
        default=None, description="Path where transformed dataset is stored."
    )
    transformation_table: Optional[str] = Field(
        default=None, description="Table name for the Transformation Layer."
    )
    transformation_partition_columns: Optional[str] = Field(
        default=None, description="Partitioning strategy for the Transformation Layer."
    )
    archive_location: Optional[str] = Field(
        default=None, description="Archive path for historical versions of the dataset."
    )
    publish_location: Optional[str] = Field(
        default=None,
        description="Final destination where the dataset is published for consumption.",
    )
    publish_table: Optional[str] = Field(
        default=None, description="Table name for the Published Layer."
    )
    publish_partition_columns: Optional[str] = Field(
        default=None, description="Partitioning strategy for the Published Layer."
    )


class ctlDataStandardisationDtl(SQLModel, table=True):
    """
    Configuration table for data standardization rules applied to specific columns.
    Includes functions and parameters for transforming raw data into standardized formats.
    """

    dataset_id: int = Field(
        primary_key=True, description="ID of the dataset being standardized."
    )
    column_name: str = Field(
        primary_key=True,
        description="Name of the column to which the standardization rule applies.",
    )
    function_name: Optional[str] = Field(
        default=None,
        description="Name of the function used for standardizing the column (e.g., trim, cast, parse_date).",
    )
    function_params: Optional[str] = Field(
        default=None,
        description="Parameters passed to the function (as JSON or string).",
    )


class logDataStandardisationDtl(SQLModel, table=True):
    """
    Logs execution details of data standardization processes.
    Tracks transformation steps and any errors that occurred.
    """

    seq_no: Optional[int] = Field(
        primary_key=True,
        default=None,
        description="Auto-incremented sequence number for logging entries.",
    )
    batch_id: Optional[int] = Field(
        default=None,
        description="Batch ID associated with the standardization process.",
    )
    process_id: Optional[int] = Field(
        default=None, description="ID of the data pipeline process involved."
    )
    dataset_id: Optional[int] = Field(
        default=None, description="ID of the dataset undergoing standardization."
    )
    source_file: Optional[str] = Field(
        default=None,
        description="Name of the source file processed during standardization.",
    )
    data_standardisation_location: Optional[str] = Field(
        default=None, description="Path where standardized output was written."
    )
    status: Optional[str] = Field(
        default=None,
        description="Current status of the standardization process (e.g., SUCCESS, FAILED).",
    )
    exception_details: Optional[str] = Field(
        default=None,
        description="Details of any exceptions encountered during processing.",
    )
    start_datetime: Optional[datetime] = Field(
        default=None, description="Start time of the standardization process."
    )
    end_datetime: Optional[datetime] = Field(
        default=None, description="End time of the standardization process."
    )


class ctlDqmMasterDtl(SQLModel, table=True):
    """
    Configuration table for Data Quality Management (DQM) rules applied to datasets.
    Defines quality checks, thresholds, and criticality levels for validation.
    """

    qc_id: int = Field(
        primary_key=True, description="Unique ID for the data quality check."
    )
    process_id: Optional[int] = Field(
        default=None, description="ID of the process associated with the QC rule."
    )
    dataset_id: Optional[int] = Field(
        default=None, description="ID of the dataset being validated."
    )
    column_name: Optional[str] = Field(
        default=None, description="Name of the column being validated."
    )
    qc_type: Optional[str] = Field(
        default=None,
        description="Type of quality check (e.g., not_null, unique, range, regex).",
    )
    qc_param: Optional[str] = Field(
        default=None,
        description="Parameters for the QC rule (e.g., min/max values, regex pattern).",
    )
    active_flag: Optional[str] = Field(
        default=None, description="Whether the QC rule is active ('Y'/'N')."
    )
    qc_filter: Optional[str] = Field(
        default=None, description="Optional filter condition for applying the QC rule."
    )
    criticality: Optional[str] = Field(
        default=None,
        description="Severity level of the QC failure (e.g., HIGH, MEDIUM, LOW).",
    )
    criticality_threshold_pct: Optional[int] = Field(
        default=None,
        description="Threshold percentage for criticality violation (e.g., 5% nulls allowed).",
    )


class logDqmDtl(SQLModel, table=True):
    """
    Logs results of data quality checks performed on datasets.
    Tracks error counts, failure thresholds, and execution times for DQM rules.
    """

    seq_no: Optional[int] = Field(
        primary_key=True,
        default=None,
        description="Auto-incremented sequence number for logging entries.",
    )
    process_id: Optional[int] = Field(
        default=None, description="ID of the data pipeline process involved."
    )
    dataset_id: Optional[int] = Field(
        default=None, description="ID of the dataset being validated."
    )
    batch_id: Optional[int] = Field(
        default=None, description="Batch ID associated with the DQM run."
    )
    source_file: Optional[str] = Field(
        default=None,
        description="Name of the source file processed during DQM validation.",
    )
    column_name: Optional[str] = Field(
        default=None, description="Name of the column being validated."
    )
    qc_type: Optional[str] = Field(
        default=None,
        description="Type of quality check performed (e.g., not_null, unique).",
    )
    qc_param: Optional[str] = Field(
        default=None,
        description="Parameters used for the QC rule (e.g., regex, value ranges).",
    )
    qc_filter: Optional[str] = Field(
        default=None, description="Filter condition applied during QC validation."
    )
    criticality: Optional[str] = Field(
        default=None,
        description="Severity level of the QC failure (e.g., HIGH, MEDIUM).",
    )
    criticality_threshold_pct: Optional[int] = Field(
        default=None, description="Threshold percentage for criticality violation."
    )
    error_count: Optional[int] = Field(
        default=None, description="Number of records failing the QC rule."
    )
    error_pct: Optional[int] = Field(
        default=None, description="Percentage of records failing the QC rule."
    )
    status: Optional[str] = Field(
        default=None, description="Status of the QC validation (e.g., PASSED, FAILED)."
    )
    dqm_start_time: Optional[datetime] = Field(
        default=None, description="Start time of the DQM validation process."
    )
    dqm_end_time: Optional[datetime] = Field(
        default=None, description="End time of the DQM validation process."
    )


class ctlTransformationDependencyMaster(SQLModel, table=True):
    """
    Stores transformation dependencies between datasets.
    Defines join logic, primary keys, and custom queries used during transformations.
    """

    process_id: int = Field(
        primary_key=True, description="ID of the transformation process."
    )
    transformation_step: Optional[str] = Field(
        default=None,
        description="Order of transformation step (e.g., 'JOIN', 'AGGREGATE').",
    )
    dataset_id: int = Field(
        primary_key=True, description="ID of the current dataset being transformed."
    )
    depedent_dataset_id: int = Field(
        primary_key=True,
        description="ID of the dependent dataset needed for transformation.",
    )
    transformation_type: Optional[str] = Field(
        default=None,
        description="Type of transformation logic (e.g., JOIN, UNION, CUSTOM).",
    )
    join_how: Optional[str] = Field(
        default=None, description="Join type (e.g., inner, left, outer)."
    )
    left_table_columns: Optional[str] = Field(
        default=None, description="Columns from the left table involved in the join."
    )
    right_table_columns: Optional[str] = Field(
        default=None, description="Columns from the right table involved in the join."
    )
    primary_keys: Optional[str] = Field(
        default=None,
        description="List of primary keys for the resulting transformed dataset.",
    )
    custom_transformation_query: Optional[str] = Field(
        default=None,
        description="Custom SQL or transformation query when using custom logic.",
    )
    extra_values: Optional[str] = Field(
        default=None,
        description="Additional parameters or metadata required for the transformation.",
    )


class logTransformationDtl(SQLModel, table=True):
    """
    Logs execution details of transformation jobs.
    Tracks performance metrics and failures related to dataset transformations.
    """

    seq_no: Optional[int] = Field(
        primary_key=True,
        default=None,
        description="Auto-incremented sequence number for logging entries.",
    )
    batch_id: Optional[int] = Field(
        default=None, description="Batch ID associated with the transformation process."
    )
    data_date: Optional[date] = Field(
        default=None,
        description="Date of the data being transformed (typically partitioning key).",
    )
    process_id: Optional[int] = Field(
        default=None, description="ID of the transformation process."
    )
    dataset_id: Optional[int] = Field(
        default=None, description="ID of the dataset undergoing transformation."
    )
    source_file: Optional[str] = Field(
        default=None,
        description="Name of the source file processed during transformation.",
    )
    status: Optional[str] = Field(
        default=None,
        description="Current status of the transformation job (e.g., SUCCESS, FAILED).",
    )
    exception_details: Optional[str] = Field(
        default=None,
        description="Details of any exceptions encountered during transformation.",
    )
    transformation_start_time: Optional[datetime] = Field(
        default=None, description="Start time of the transformation job."
    )
    transformation_end_time: Optional[datetime] = Field(
        default=None, description="End time of the transformation job."
    )
