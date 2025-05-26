import polars
from os import getenv
from dotenv import load_dotenv
from typing import Union, Literal, Optional

load_dotenv()

aws_key = getenv("aws_key")
aws_secret = getenv("aws_secret")
aws_endpoint = getenv("aws_endpoint")


storage_options = {
    "AWS_ACCESS_KEY_ID": aws_key,
    "AWS_SECRET_ACCESS_KEY": aws_secret,
    "AWS_ENDPOINT_URL": aws_endpoint,
}


class BronzeInboundWriter:
    """
    Writes input data to a CSV or TXT file at the specified location.

    This class supports writing either a Polars DataFrame or a list of dictionaries
    to a `.csv` or `.txt` file using a specified delimiter. If the input is a dictionary list,
    it will be automatically converted to a Polars DataFrame before writing.

    Attributes:
        input_data (Union[polars.DataFrame, List[Dict]]): Data to write to file.
        save_location (str): Full path including filename where the file should be saved.
        outbound_file_delimiter (str): Delimiter used when writing the file (e.g., ',', '\t').

    Raises:
        ValueError: If an unsupported file format is provided in `save_location`.

    Examples:
            >>> data = [{"name": "Alice", "age": 30}, {"name": "Bob", "age": 25}]
            >>> writer = BronzeInboundWriter(input_data=data, save_location="/path/to/output.csv", outbound_file_delimiter=",")
    """

    def __init__(
        self,
        input_data: Union[polars.DataFrame, list[dict]],
        save_location: str,
        outbound_file_delimiter: str,
    ):
        """
        Write input data to a CSV or TXT file at the specified location.

        This constructor checks the file extension in `save_location` (e.g., .csv or .txt)
        and writes the provided `input_data` using Polars. If the input is a dictionary list,
        it will be converted to a DataFrame before writing.

        Args:
            input_data (Union[polars.DataFrame, list[dict]]): The data to write to file.
                Can be either a Polars DataFrame or a list of dictionaries.
            save_location (str): Full path (including filename) where the file should be saved.
                Supported extensions: `.csv`, `.txt`.
            outbound_file_delimiter (str): Delimiter to use when writing the file (e.g., ',', '\t').

        Raises:
            ValueError: If an unsupported file format is provided in `save_location`.

        Examples:
            >>> data = [{"name": "Alice", "age": 30}, {"name": "Bob", "age": 25}]
            >>> writer = BronzeInboundWriter(input_data=data, save_location="/path/to/output.csv", outbound_file_delimiter=",")
        """
        if "csv" in save_location or "txt" in save_location:

            if isinstance(input_data, polars.DataFrame):
                input_data.write_csv(
                    file=save_location,
                    separator=outbound_file_delimiter,
                    storage_options=storage_options,
                )

            else:
                df = polars.DataFrame(input_data)
                df.write_csv(
                    file=save_location,
                    separator=outbound_file_delimiter,
                    storage_options=storage_options,
                )


class DeltaTableWriter:
    def __init__(
        self,
        input_data: Union[polars.DataFrame, list[dict], str],
        save_location: str,
        batch_id: int,
        partition_columns: str,
        outbound_file_delimiter: Optional[str] = None,
        infer_schema: Optional[bool] = False,
    ):
        """
        Write data to a Delta Lake table with optional partitioning and batch ID tagging.

        This class supports writing to Delta Lake from the following sources:
        - Polars DataFrame
        - List of dictionaries (converted into DataFrame)
        - CSV/TXT file path (read into DataFrame)

        A `batch_id` column is added to all records for traceability. Data is written in append mode.

        Args:
            input_data (Union[polars.DataFrame, list[dict], str]): The data to write.
                Can be a DataFrame, list of dicts, or a file path to CSV/TXT.
            save_location (str): Target location where the Delta table will be saved.
                Supports local paths or cloud storage paths (e.g., S3).
            batch_id (int): Batch ID to tag all rows with for tracking purposes.
            partition_columns (str): Comma-separated string of columns to partition by.
            outbound_file_delimiter (Optional[str], optional): Delimiter used in the source file
                (if `input_data` is a file path). Defaults to None.
            infer_schema (Optional[bool], optional): Whether to infer schema when reading CSV/TXT.
                Defaults to False.

        Raises:
            ValueError: If unsupported file format is provided or required parameters are missing.

        Examples:
            >>> writer = DeltaTableWriter(
            ...     input_data="data.csv",
            ...     save_location="s3://bucket/delta_table",
            ...     batch_id=123,
            ...     partition_columns="country,date",
            ...     outbound_file_delimiter=","
            ... )
        """

        if isinstance(input_data, polars.DataFrame):
            input_data = input_data.with_columns(polars.lit(batch_id).alias("batch_id"))
            input_data.write_delta(
                save_location,
                storage_options=storage_options,
                mode="append",
                delta_write_options={"partition_by": partition_columns.split(",")},
            )
        elif isinstance(input_data, str):
            if "csv" in input_data or "txt" in input_data:
                df = polars.read_csv(
                    input_data,
                    separator=outbound_file_delimiter,
                    infer_schema=infer_schema,
                    storage_options={
                        "key": aws_key,
                        "secret": aws_secret,
                        "client_kwargs": {"endpoint_url": aws_endpoint},
                    },
                )
                df = df.with_columns(polars.lit(batch_id).alias("batch_id"))
                df.write_delta(
                    save_location,
                    storage_options=storage_options,
                    mode="append",
                    delta_write_options={"partition_by": partition_columns.split(",")},
                )

        else:
            df = polars.DataFrame(input_data)
            df.with_columns(polars.lit(batch_id).alias("batch_id"))
            df.write_delta(
                file=save_location,
                storage_options=storage_options,
                mode="append",
                delta_write_options={"partition_by": partition_columns.split(",")},
            )


class DeltaTablePublishWrite:
    """
    A class to publish a Polars DataFrame to a Delta Lake table with optional batch ID tagging.

    This class writes a DataFrame to a Delta Lake table, supporting:
    - Partitioning by one or more columns
    - Optional addition of a `batch_id` column for traceability
    - Overwrite mode for idempotent publishing
    """

    def __init__(
        self,
        input_data: polars.DataFrame,
        save_location: str,
        partition_columns: str,
        batch_id: Optional[int] = None,
    ):
        """
        Initialize the Delta table write operation.

        Writes the provided DataFrame to the specified Delta Lake location. If a `batch_id`
        is provided, it adds a new column to the DataFrame before writing.

        Args:
            input_data (polars.DataFrame): The DataFrame to be written to Delta Lake.
            save_location (str): Target location where the Delta table will be saved.
            partition_columns (str): Comma-separated string of columns to partition by.
            batch_id (Optional[int], optional): Optional batch identifier added as a column.
                Defaults to None.

        Examples:
            >>> df = pl.DataFrame({"name": ["Alice", "Bob"], "age": [30, 25]})
            >>> writer = DeltaTablePublishWrite(
            ...     input_data=df,
            ...     save_location="s3://bucket/published_table",
            ...     partition_columns="age",
            ...     batch_id=123
            ... )
        """
        if not batch_id:
            input_data.write_delta(
                save_location,
                storage_options=storage_options,
                mode="overwrite",
                delta_write_options={"partition_by": partition_columns.split(",")},
            )
        else:
            input_data.with_columns(polars.lit(batch_id).alias("batch_id")).write_delta(
                save_location,
                storage_options=storage_options,
                mode="overwrite",
                delta_write_options={"partition_by": partition_columns.split(",")},
            )


class DeltaTableRead:
    """
    A class to read data from a Delta Lake table with optional filtering by batch ID or latest version.

    This class provides flexible reading capabilities from a Delta Lake table, including:
    - Reading all data
    - Filtering by a specific `batch_id`
    - Reading only the latest batch of data
    """

    def __init__(
        self,
        delta_path: str,
        batch_id: Optional[int] = None,
        latest: bool = False,
    ):
        self.delta_path = delta_path
        self.batch_id = batch_id
        self.latest = latest

    def read(self):
        """
        Initialize the DeltaTableRead instance with read parameters.

        Args:
            delta_path (str): Path to the Delta Lake table.
            batch_id (Optional[int], optional): Specific batch ID to filter data. Defaults to None.
            latest (bool, optional): Whether to fetch only the latest batch. Defaults to False.
        """
        if self.batch_id:
            df = polars.scan_delta(self.delta_path, storage_options=storage_options)
            df = df.filter(polars.col("batch_id") == self.batch_id)
            return df.collect()
        else:
            if self.latest:
                df = polars.scan_delta(self.delta_path, storage_options=storage_options)
                max_batch_id = df.select(polars.col("batch_id").max()).collect().item()

                return df.filter(polars.col("batch_id") == max_batch_id).collect()
            else:
                return polars.read_delta(
                    self.delta_path, storage_options=storage_options
                )


class DeltaTableWriterScdType2:
    """
    A class to perform SCD Type 2 (Slowly Changing Dimension Type 2) upserts into a Delta Lake table.

    This class writes data from a staging DataFrame into a Delta Lake table using Delta Merge operations.
    It handles both:
    - Updating existing records when changes are detected (`when_matched_update`)
    - Inserting new records that do not already exist (`when_not_matched_insert_all`)

    The implementation assumes the presence of specific columns for versioning:
    - `eff_strt_dt`: Effective start date of the record
    - `eff_end_dt`: Effective end date of the record (set to '9999-12-31' for active records)
    - `sys_checksum`: Column used to detect changes (e.g., hash of relevant fields)
    """

    def __init__(
        self, staging_df: polars.DataFrame, delta_path: str, primary_keys: str
    ):
        """
        Initialize and execute an SCD Type 2 merge operation on a Delta Lake table.

        Performs two-phase write logic:
        1. Updates existing active records if they have changed.
        2. Inserts new records as active entries with `eff_end_dt = '9999-12-31'`.

        Args:
            staging_df (polars.DataFrame): Incoming data containing staged changes.
            delta_path (str): Path to the target Delta Lake table.
            primary_keys (str): SQL-like string expression representing the join condition,
                e.g., `"target.id = staging.id"`.

        Examples:
            >>> writer = DeltaTableWriterScdType2(
            ...     staging_df=staging_data,
            ...     delta_path="s3://bucket/table",
            ...     primary_keys="target.id = staging.id"
            ... )
        """

        staging_df.write_delta(
            delta_path,
            storage_options=storage_options,
            mode="merge",
            delta_merge_options={
                "source_alias": "staging",
                "target_alias": "target",
                "predicate": f"target.eff_end_dt == '9999-12-31' AND {primary_keys}",
            },
        ).when_matched_update(
            predicate="target.sys_checksum != staging.sys_checksum",
            updates={
                "eff_end_dt": "staging.eff_strt_dt",
                "sys_del_flg": "'Y'",
            },
        ).when_not_matched_insert_all().execute()

        staging_df.write_delta(
            delta_path,
            storage_options=storage_options,
            mode="merge",
            delta_merge_options={
                "source_alias": "staging",
                "target_alias": "target",
                "predicate": f"target.eff_end_dt == '9999-12-31' AND {primary_keys}",
            },
        ).when_not_matched_insert_all().execute()


class DeltaTableStreamlitAdapter:
    """
    Adapter class to read Delta Lake tables for visualization in Streamlit applications.

    This class provides a simple interface to load data from a Delta Lake table using lazy evaluation
    via Polars' `scan_delta`, making it suitable for large datasets commonly used in dashboards.
    """

    def __init__(
        self,
        delta_path: str,
    ):
        """
        Initialize the adapter with the path to the Delta Lake table.

        Args:
            delta_path (str): Path to the Delta Lake table that will be queried.
        """
        self.delta_path = delta_path

    def read(self):
        """
        Read the Delta Lake table into a LazyFrame for deferred execution.

        Returns:
            polars.LazyFrame: A lazy representation of the Delta Lake table,
            suitable for efficient querying and visualization in Streamlit.
        """
        return polars.scan_delta(self.delta_path, storage_options=storage_options)
