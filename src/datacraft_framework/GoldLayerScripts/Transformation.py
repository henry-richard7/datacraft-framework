import polars
import polars_hash as plh

from os import getenv
from dotenv import load_dotenv

from datacraft_framework.Common.OrchestrationProcess import OrchestrationProcess
from datacraft_framework.Models.schema import ctlDatasetMaster, logTransformationDtl
from datacraft_framework.Common.S3Process import S3Process, path_to_s3
from datacraft_framework.Common.DataProcessor import (
    DeltaTableRead,
    DeltaTableWriter,
    DeltaTableWriterScdType2,
)
from datetime import datetime
import logging
import traceback

logger = logging.getLogger(__name__)

load_dotenv()
env = getenv("env")


class Transformation:
    """
    A class to orchestrate and execute data transformations from SILVER to Gold layers.

    This class supports multiple transformation strategies:
        - Direct mapping of source to target
        - Union of multiple datasets
        - Join-based transformations
        - Custom SQL queries executed via Polars SQLContext

    The class also handles:
        - Adding system fields (`sys_checksum`, `eff_strt_dt`, etc.)
        - Writing transformed data to Delta Lake
        - Logging transformation status

    Attributes:
        dataset_master (ctlDatasetMaster): The master details of a dataset.
    """

    def __init__(self, dataset_master: ctlDatasetMaster):
        """
        Initialize and dispatch transformation based on configuration.

        Args:
            dataset_master (ctlDatasetMaster): Configuration object containing transformation rules.

        Raises:
            Exception: If an unsupported transformation type is requested.
        """
        self.dataset_master = dataset_master

        with OrchestrationProcess() as orch_process:
            transformation_depedencies = (
                orch_process.get_transformation_dependency_master(
                    process_id=dataset_master.process_id,
                    dataset_id=dataset_master.dataset_id,
                )
            )

            if transformation_depedencies[0].transformation_type.lower() == "direct":
                self.direct_transformation()
            elif transformation_depedencies[0].transformation_type.lower() == "join":
                self.join_transformation()
            elif transformation_depedencies[0].transformation_type.lower() == "union":
                self.union_transformation()
            elif transformation_depedencies[0].transformation_type.lower() == "custom":
                self.custom_transformation()
            else:
                raise Exception(
                    f"Unsupported Transformation: {transformation_depedencies[0].transformation_type}"
                )

    def direct_transformation(self):
        """
        Perform a direct transformation: read from one SILVER table and write to a Gold table.

        Applies system columns and writes to Delta Lake. Uses SCD Type 2 upserts if the target exists.
        """
        dataset_master = self.dataset_master

        with OrchestrationProcess() as orch_process:
            transformation_dependency = (
                orch_process.get_transformation_dependency_master(
                    dataset_id=dataset_master.dataset_id,
                    process_id=dataset_master.process_id,
                )[0]
            )

            dependent_dataset_id = transformation_dependency.depedent_dataset_id
            primary_keys = transformation_dependency.primary_keys.split(",")
            primary_key_conditions = " AND ".join(
                [f"target.{key} = staging.{key}" for key in primary_keys]
            )

            columns = orch_process.get_ctl_column_metadata(
                dataset_id=dataset_master.dataset_id
            )
            columns = [x.column_name for x in columns]

            target_table_details = orch_process.get_dataset_master(
                process_id=dataset_master.process_id,
                dataset_type="GOLD",
                dataset_id=dataset_master.dataset_id,
            )
            dependent_table_details = orch_process.get_dataset_master(
                process_id=dataset_master.process_id,
                dataset_type="BRONZE",
                dataset_id=dependent_dataset_id,
            )

            unprocessed_transformation_files = (
                orch_process.get_unprocessed_transformation_files(
                    process_id=dataset_master.process_id,
                    dataset_id=dependent_dataset_id,
                )
            )

            dependent_table_details_s3 = path_to_s3(
                location=dependent_table_details.staging_location,
                env=env,
            )
            target_table_details_s3 = path_to_s3(
                location=target_table_details.transformation_location,
                env=env,
            )

            if len(unprocessed_transformation_files) == 0:
                raise Exception(
                    f"No unprocess files found for Dataset ID {dataset_master.dataset_id}"
                )
            else:
                for unprocessed_file in unprocessed_transformation_files:
                    start_time = datetime.now()

                    batch_id = unprocessed_file.batch_id

                    try:
                        staging_df = DeltaTableRead(
                            delta_path=dependent_table_details_s3["s3_location"],
                            batch_id=batch_id,
                        ).read()

                        staging_df = staging_df.drop(polars.col("batch_id"))
                        staging_df = staging_df.select(columns)

                        staging_df = staging_df.with_columns(
                            [
                                polars.lit(datetime.now().date()).alias("data_date"),
                                polars.lit(batch_id).alias("batch_id"),
                                polars.lit(datetime.now().date()).alias("eff_strt_dt"),
                                polars.lit("N").alias("sys_del_flg"),
                                polars.lit(
                                    datetime(year=9999, month=12, day=31).date()
                                ).alias("eff_end_dt"),
                                polars.lit(datetime.now()).alias("sys_created_ts"),
                                polars.lit(datetime.now()).alias("sys_modified_ts"),
                                plh.concat_str(*columns)
                                .chash.sha256()
                                .alias("sys_checksum"),
                            ]
                        )

                        if S3Process().s3_list_files(
                            bucket=target_table_details_s3["bucket"],
                            file_name=target_table_details_s3["key"],
                        ):
                            # Upsert logic here
                            DeltaTableWriterScdType2(
                                staging_df=staging_df,
                                primary_keys=primary_key_conditions,
                                delta_path=target_table_details_s3["s3_location"],
                            )
                        else:
                            DeltaTableWriter(
                                input_data=staging_df,
                                save_location=target_table_details_s3["s3_location"],
                                batch_id=batch_id,
                                partition_columns=target_table_details.transformation_partition_columns,
                            )
                        orch_process.insert_log_transformation(
                            log_transformation=logTransformationDtl(
                                batch_id=batch_id,
                                data_date=start_time.date(),
                                process_id=dataset_master.process_id,
                                dataset_id=dataset_master.dataset_id,
                                source_file=unprocessed_file.source_file,
                                status="SUCCEEDED",
                                exception_details=None,
                                transformation_start_time=start_time,
                                transformation_end_time=datetime.now(),
                            )
                        )
                    except Exception as e:
                        orch_process.insert_log_transformation(
                            log_transformation=logTransformationDtl(
                                batch_id=batch_id,
                                data_date=start_time.date(),
                                process_id=dataset_master.process_id,
                                dataset_id=dataset_master.dataset_id,
                                source_file=unprocessed_file.source_file,
                                status="FAILED",
                                exception_details=traceback.format_exc(),
                                transformation_start_time=start_time,
                                transformation_end_time=datetime.now(),
                            )
                        )
                        logger.error(traceback.format_exc())
                        raise

    def union_transformation(self):
        """
        Perform a union transformation across multiple source datasets.

        Combines records from multiple sources into a single Gold table.
        Adds system columns and writes result using Delta Lake.
        """
        dataset_master = self.dataset_master

        with OrchestrationProcess() as orch_process:
            transformation_depedencies = (
                orch_process.get_transformation_dependency_master(
                    dataset_id=dataset_master.dataset_id,
                    process_id=dataset_master.process_id,
                )
            )

            primary_keys = transformation_depedencies[0].primary_keys.split(",")
            primary_key_conditions = " AND ".join(
                [f"target.{key} = staging.{key}" for key in primary_keys]
            )

            columns = orch_process.get_ctl_column_metadata(
                dataset_id=dataset_master.dataset_id
            )
            columns = [x.column_name for x in columns]

            target_table_details = orch_process.get_dataset_master(
                process_id=dataset_master.process_id,
                dataset_type="GOLD",
                dataset_id=dataset_master.dataset_id,
            )

            target_table_details_s3 = path_to_s3(
                location=target_table_details.transformation_location,
                env=env,
            )

            unprocessed_transformation_files = (
                orch_process.get_unprocessed_transformation_files(
                    process_id=dataset_master.process_id,
                    dataset_id=transformation_depedencies[0].depedent_dataset_id,
                )
            )

            if len(unprocessed_transformation_files) == 0:
                raise Exception(
                    f"No unprocess files found for Dataset ID {dataset_master.dataset_id}"
                )
            else:
                source_details = list()

                for transformation_depedency in transformation_depedencies:
                    dependent_dataset_details = orch_process.get_dataset_master(
                        process_id=dataset_master.process_id,
                        dataset_id=transformation_depedency.depedent_dataset_id,
                        dataset_type="BRONZE",
                    )

                    source_details.append(
                        {
                            "source_table_location": dependent_dataset_details.staging_location,
                            "source_table_name": dependent_dataset_details.staging_table,
                            "extra_value": transformation_depedency.extra_values,
                        }
                    )

                for unprocessed_file in unprocessed_transformation_files:
                    start_time = datetime.now()
                    batch_id = unprocessed_file.batch_id

                    try:
                        source_dfs: list[polars.DataFrame] = list()

                        for source_detail in source_details:
                            source_table_location = source_detail[
                                "source_table_location"
                            ]
                            source_table_location_s3 = path_to_s3(
                                location=source_table_location, env=env
                            )

                            if source_detail.get("extra_values"):
                                new_values = [
                                    polars.lit(value.strip("'")).alias(column_name)
                                    for column_name, value in (
                                        item.split("=")
                                        for item in source_detail["extra_values"].split(
                                            ","
                                        )
                                    )
                                ]

                                source_dfs.append(
                                    DeltaTableRead(
                                        delta_path=source_table_location_s3[
                                            "s3_location"
                                        ],
                                        latest=True,
                                    )
                                    .read()
                                    .with_columns(new_values)
                                )
                            else:
                                source_dfs.append(
                                    DeltaTableRead(
                                        delta_path=source_table_location_s3[
                                            "s3_location"
                                        ],
                                        latest=True,
                                    ).read()
                                )

                        result_df = polars.concat(source_dfs).select(columns)
                        final_df = result_df.with_columns(
                            [
                                polars.lit(datetime.now().date()).alias("data_date"),
                                polars.lit(batch_id).alias("batch_id"),
                                polars.lit(datetime.now().date()).alias("eff_strt_dt"),
                                polars.lit("N").alias("sys_del_flg"),
                                polars.lit(
                                    datetime(year=9999, month=12, day=31).date()
                                ).alias("eff_end_dt"),
                                polars.lit(datetime.now()).alias("sys_created_ts"),
                                polars.lit(datetime.now()).alias("sys_modified_ts"),
                                plh.concat_str(*columns)
                                .chash.sha256()
                                .alias("sys_checksum"),
                            ]
                        )

                        if S3Process().s3_list_files(
                            bucket=target_table_details_s3["bucket"],
                            file_name=target_table_details_s3["key"],
                        ):
                            # Upsert logic here
                            DeltaTableWriterScdType2(
                                staging_df=final_df,
                                primary_keys=primary_key_conditions,
                                delta_path=target_table_details_s3["s3_location"],
                            )
                        else:
                            DeltaTableWriter(
                                input_data=final_df,
                                save_location=target_table_details_s3["s3_location"],
                                batch_id=batch_id,
                                partition_columns=target_table_details.transformation_partition_columns,
                            )
                        orch_process.insert_log_transformation(
                            log_transformation=logTransformationDtl(
                                batch_id=batch_id,
                                data_date=start_time.date(),
                                process_id=dataset_master.process_id,
                                dataset_id=dataset_master.dataset_id,
                                source_file=unprocessed_file.source_file,
                                status="SUCCEEDED",
                                exception_details=None,
                                transformation_start_time=start_time,
                                transformation_end_time=datetime.now(),
                            )
                        )
                    except Exception as e:
                        orch_process.insert_log_transformation(
                            log_transformation=logTransformationDtl(
                                batch_id=batch_id,
                                data_date=start_time.date(),
                                process_id=dataset_master.process_id,
                                dataset_id=dataset_master.dataset_id,
                                source_file=unprocessed_file.source_file,
                                status="FAILED",
                                exception_details=traceback.format_exc(),
                                transformation_start_time=start_time,
                                transformation_end_time=datetime.now(),
                            )
                        )
                        logger.error(traceback.format_exc())
                        raise

    def join_transformation(self):
        """
        Perform a join-based transformation between multiple SILVER datasets.

        Executes SQL-like joins between datasets and writes the result to Gold layer.
        """
        dataset_master = self.dataset_master

        with OrchestrationProcess() as orch_process:
            transformation_depedencies = (
                orch_process.get_transformation_dependency_master(
                    dataset_id=dataset_master.dataset_id,
                    process_id=dataset_master.process_id,
                )
            )

            primary_keys = transformation_depedencies[0].primary_keys.split(",")
            primary_key_conditions = " AND ".join(
                [f"target.{key} = staging.{key}" for key in primary_keys]
            )

            columns = orch_process.get_ctl_column_metadata(
                dataset_id=dataset_master.dataset_id
            )
            columns = [x.column_name for x in columns]

            target_table_details = orch_process.get_dataset_master(
                process_id=dataset_master.process_id,
                dataset_type="GOLD",
                dataset_id=dataset_master.dataset_id,
            )

            target_table_details_s3 = path_to_s3(
                location=target_table_details.transformation_location,
                env=env,
            )

            unprocessed_transformation_files = (
                orch_process.get_unprocessed_transformation_files(
                    process_id=dataset_master.process_id,
                    dataset_id=transformation_depedencies[0].depedent_dataset_id,
                )
            )

            if len(unprocessed_transformation_files) == 0:
                raise Exception(
                    f"No unprocess files found for Dataset ID {dataset_master.dataset_id}"
                )
            else:
                source_details = list()

                for transformation_depedency in transformation_depedencies:
                    dependent_dataset_details = orch_process.get_dataset_master(
                        process_id=dataset_master.process_id,
                        dataset_id=transformation_depedency.depedent_dataset_id,
                        dataset_type="BRONZE",
                    )
                    df_source = (
                        DeltaTableRead(
                            path_to_s3(
                                location=dependent_dataset_details.staging_location,
                                env=env,
                            )["s3_location"],
                            latest=True,
                        )
                        .read()
                        .drop("batch_id")
                    )

                    source_details.append(
                        {
                            "table_name": dependent_dataset_details.staging_table,
                            "dataframe": df_source,
                            "how": transformation_depedency.join_how,
                            "left_join_columns": transformation_depedency.left_table_columns,
                            "right_join_columns": transformation_depedency.right_table_columns,
                            "dependent_dataset_id": transformation_depedency.depedent_dataset_id,
                        }
                    )

                for unprocessed_file in unprocessed_transformation_files:
                    start_time = datetime.now()
                    batch_id = unprocessed_file.batch_id

                    try:
                        base_df = source_details[0]["dataframe"]

                        for entry in source_details[1:]:
                            df_right = entry["dataframe"]
                            how = entry["how"].lower()
                            left_on = entry["left_join_columns"].split(",")
                            right_on = entry["right_join_columns"].split(",")

                            # Safety check
                            if len(left_on) != len(right_on):
                                raise ValueError(
                                    f"Join key count mismatch: {left_on} vs {right_on}"
                                )

                            base_df = base_df.join(
                                df_right, left_on=left_on, right_on=right_on, how=how
                            )

                        final_df = base_df.with_columns(
                            [
                                polars.lit(datetime.now().date()).alias("data_date"),
                                polars.lit(batch_id).alias("batch_id"),
                                polars.lit(datetime.now().date()).alias("eff_strt_dt"),
                                polars.lit("N").alias("sys_del_flg"),
                                polars.lit(
                                    datetime(year=9999, month=12, day=31).date()
                                ).alias("eff_end_dt"),
                                polars.lit(datetime.now()).alias("sys_created_ts"),
                                polars.lit(datetime.now()).alias("sys_modified_ts"),
                                plh.concat_str(*columns)
                                .chash.sha256()
                                .alias("sys_checksum"),
                            ]
                        )

                        if S3Process().s3_list_files(
                            bucket=target_table_details_s3["bucket"],
                            file_name=target_table_details_s3["key"],
                        ):
                            # Upsert logic here
                            DeltaTableWriterScdType2(
                                staging_df=final_df,
                                primary_keys=primary_key_conditions,
                                delta_path=target_table_details_s3["s3_location"],
                            )
                        else:
                            DeltaTableWriter(
                                input_data=final_df,
                                save_location=target_table_details_s3["s3_location"],
                                batch_id=batch_id,
                                partition_columns=target_table_details.transformation_partition_columns,
                            )

                        orch_process.insert_log_transformation(
                            log_transformation=logTransformationDtl(
                                batch_id=batch_id,
                                data_date=start_time.date(),
                                process_id=dataset_master.process_id,
                                dataset_id=dataset_master.dataset_id,
                                source_file=unprocessed_file.source_file,
                                status="SUCCEEDED",
                                exception_details=None,
                                transformation_start_time=start_time,
                                transformation_end_time=datetime.now(),
                            )
                        )

                    except Exception as e:
                        orch_process.insert_log_transformation(
                            log_transformation=logTransformationDtl(
                                batch_id=batch_id,
                                data_date=start_time.date(),
                                process_id=dataset_master.process_id,
                                dataset_id=dataset_master.dataset_id,
                                source_file=unprocessed_file.source_file,
                                status="FAILED",
                                exception_details=traceback.format_exc(),
                                transformation_start_time=start_time,
                                transformation_end_time=datetime.now(),
                            )
                        )
                        logger.error(traceback.format_exc())
                        raise

    def custom_transformation(self):
        """
        Perform a custom transformation using SQL queries.

        Reads data from one or more SILVER datasets and executes a user-defined SQL query
        to produce the final Gold dataset.
        """
        dataset_master = self.dataset_master

        with OrchestrationProcess() as orch_process:
            transformation_depedencies = (
                orch_process.get_transformation_dependency_master(
                    dataset_id=dataset_master.dataset_id,
                    process_id=dataset_master.process_id,
                )
            )

            primary_keys = transformation_depedencies[0].primary_keys.split(",")
            primary_key_conditions = " AND ".join(
                [f"target.{key} = staging.{key}" for key in primary_keys]
            )

            columns = orch_process.get_ctl_column_metadata(
                dataset_id=dataset_master.dataset_id
            )
            columns = [x.column_name for x in columns]

            target_table_details = orch_process.get_dataset_master(
                process_id=dataset_master.process_id,
                dataset_type="GOLD",
                dataset_id=dataset_master.dataset_id,
            )

            target_table_details_s3 = path_to_s3(
                location=target_table_details.transformation_location,
                env=env,
            )

            unprocessed_transformation_files = (
                orch_process.get_unprocessed_transformation_files(
                    process_id=dataset_master.process_id,
                    dataset_id=transformation_depedencies[0].depedent_dataset_id,
                )
            )

            if len(unprocessed_transformation_files) == 0:
                raise Exception(
                    f"No unprocess files found for Dataset ID {dataset_master.dataset_id}"
                )
            else:
                source_details = list()

                for transformation_depedency in transformation_depedencies:
                    dependent_dataset_details = orch_process.get_dataset_master(
                        process_id=dataset_master.process_id,
                        dataset_id=transformation_depedency.depedent_dataset_id,
                        dataset_type="BRONZE",
                    )
                    df_source = (
                        DeltaTableRead(
                            path_to_s3(
                                location=dependent_dataset_details.staging_location,
                                env=env,
                            )["s3_location"],
                            latest=True,
                        )
                        .read()
                        .drop("batch_id")
                    )

                    source_details.append(
                        {
                            "table_name": dependent_dataset_details.staging_table,
                            "dataframe": df_source,
                        }
                    )

                for unprocessed_file in unprocessed_transformation_files:
                    start_time = datetime.now()
                    try:
                        batch_id = unprocessed_file.batch_id

                        sql_context = polars.SQLContext()
                        for source_detail in source_details:
                            sql_context.register(
                                name=source_detail["table_name"],
                                frame=source_detail["dataframe"],
                            )

                        result_df = sql_context.execute(
                            transformation_depedencies[-1].custom_transformation_query
                        ).collect()
                        final_df = result_df.with_columns(
                            [
                                polars.lit(datetime.now().date()).alias("data_date"),
                                polars.lit(batch_id).alias("batch_id"),
                                polars.lit(datetime.now().date()).alias("eff_strt_dt"),
                                polars.lit("N").alias("sys_del_flg"),
                                polars.lit(
                                    datetime(year=9999, month=12, day=31).date()
                                ).alias("eff_end_dt"),
                                polars.lit(datetime.now()).alias("sys_created_ts"),
                                polars.lit(datetime.now()).alias("sys_modified_ts"),
                                plh.concat_str(*columns)
                                .chash.sha256()
                                .alias("sys_checksum"),
                            ]
                        )

                        if S3Process().s3_list_files(
                            bucket=target_table_details_s3["bucket"],
                            file_name=target_table_details_s3["key"],
                        ):
                            # Upsert logic here
                            DeltaTableWriterScdType2(
                                staging_df=final_df,
                                primary_keys=primary_key_conditions,
                                delta_path=target_table_details_s3["s3_location"],
                            )
                        else:
                            DeltaTableWriter(
                                input_data=final_df,
                                save_location=target_table_details_s3["s3_location"],
                                batch_id=batch_id,
                                partition_columns=target_table_details.transformation_partition_columns,
                            )

                        orch_process.insert_log_transformation(
                            log_transformation=logTransformationDtl(
                                batch_id=batch_id,
                                data_date=start_time.date(),
                                process_id=dataset_master.process_id,
                                dataset_id=dataset_master.dataset_id,
                                source_file=unprocessed_file.source_file,
                                status="SUCCEEDED",
                                exception_details=None,
                                transformation_start_time=start_time,
                                transformation_end_time=datetime.now(),
                            )
                        )
                    except Exception as e:
                        orch_process.insert_log_transformation(
                            log_transformation=logTransformationDtl(
                                batch_id=batch_id,
                                data_date=start_time.date(),
                                process_id=dataset_master.process_id,
                                dataset_id=dataset_master.dataset_id,
                                source_file=unprocessed_file.source_file,
                                status="FAILED",
                                exception_details=traceback.format_exc(),
                                transformation_start_time=start_time,
                                transformation_end_time=datetime.now(),
                            )
                        )
                        logger.error(traceback.format_exc())
                        raise
