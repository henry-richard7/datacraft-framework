from json import loads as json_loads
from datetime import datetime
import polars

from datacraft_framework.Common.OrchestrationProcess import OrchestrationProcess
from datacraft_framework.Common.S3Process import S3Process, path_to_s3
from datacraft_framework.Common.DataProcessor import DeltaTableRead, DeltaTableWriter
from datacraft_framework.Common.RegexDateFormats import get_date_regex

from datacraft_framework.Models.schema import (
    ctlDatasetMaster,
    ctlDqmMasterDtl,
    logDqmDtl,
)

from os import getenv
from dotenv import load_dotenv

import logging
import traceback

logger = logging.getLogger(__name__)

load_dotenv()
env = getenv("env")


class DataQualityCheck:
    def null_check(
        self,
        df: polars.DataFrame,
        batch_id: int,
        source_file: str,
        dqm_detail: ctlDqmMasterDtl,
        dataset_master: ctlDatasetMaster,
        orch_process: OrchestrationProcess,
    ) -> polars.DataFrame:
        """
        Perform null value check on specified column.

        Filters rows where the target column contains non-null values.
        Logs results into `logDqmDtl` table and optionally raises exception if criticality is breached.

        Args:
            df (polars.DataFrame): Input DataFrame to validate.
            batch_id (int): Batch ID associated with this data.
            source_file (str): Source file name for logging purposes.
            dqm_detail (ctlDqmMasterDtl): DQM rule definition object.
            dataset_master (ctlDatasetMaster): Dataset metadata object.
            orch_process (OrchestrationProcess): Instance used for logging.

        Returns:
            polars.DataFrame: Filtered DataFrame containing only valid (non-null) records.

        Raises:
            Exception: If null values exceed criticality threshold and marked 'C' (Critical).
        """

        start_time = datetime.now()
        total_count = len(df)

        if dqm_detail.qc_filter:
            filter_cols = dqm_detail.qc_filter.split(",")
            filter_cols = " AND ".join(filter_cols)
            null_check_dqm_ = df.filter(
                polars.col(dqm_detail.column_name).is_not_null()
            ).sql(f"SELECT * FROM self WHERE {filter_cols}")
        else:
            null_check_dqm_ = df.filter(
                polars.col(dqm_detail.column_name).is_not_null()
            )

        failure_count = total_count - len(null_check_dqm_)

        if failure_count != 0:
            failure_percentage = (failure_count / total_count) * 100
            failed_records = df.join(null_check_dqm_, on=df.columns, how="anti")

            if (dqm_detail.criticality == "C") and (
                failure_percentage >= dqm_detail.criticality_threshold_pct
            ):
                orch_process.insert_log_dqm(
                    log_dqm=logDqmDtl(
                        process_id=dataset_master.dataset_id,
                        dataset_id=dataset_master.dataset_id,
                        batch_id=batch_id,
                        source_file=source_file,
                        column_name=dqm_detail.column_name,
                        qc_type=dqm_detail.qc_type,
                        qc_param=dqm_detail.qc_param,
                        qc_filter=dqm_detail.qc_param,
                        criticality=dqm_detail.criticality,
                        criticality_threshold_pct=dqm_detail.criticality_threshold_pct,
                        error_count=failure_count,
                        error_pct=failure_percentage,
                        status="FAILED",
                        dqm_start_time=start_time,
                        dqm_end_time=datetime.now(),
                    )
                )
                logger.error(
                    f"Dataset ID: {dataset_master.dataset_id} DQM check Failed For: {dqm_detail.qc_type} as it crossed Criticality Threshold {failure_percentage}%."
                )
                raise Exception(
                    f"DQM Checks failed for dataset ID: {dataset_master.dataset_id} for {dqm_detail.qc_type}-Check as it crossed Criticality Threshold {failure_percentage}%."
                )
            elif (dqm_detail.criticality == "C") and (
                failure_percentage <= dqm_detail.criticality_threshold_pct
            ):
                # Write Passed Records only
                orch_process.insert_log_dqm(
                    log_dqm=logDqmDtl(
                        process_id=dataset_master.dataset_id,
                        dataset_id=dataset_master.dataset_id,
                        batch_id=batch_id,
                        source_file=source_file,
                        column_name=dqm_detail.column_name,
                        qc_type=dqm_detail.qc_type,
                        qc_param=dqm_detail.qc_param,
                        qc_filter=dqm_detail.qc_param,
                        criticality=dqm_detail.criticality,
                        criticality_threshold_pct=dqm_detail.criticality_threshold_pct,
                        error_count=failure_count,
                        error_pct=failure_percentage,
                        status="SUCCEEDED",
                        dqm_start_time=start_time,
                        dqm_end_time=datetime.now(),
                    )
                )
                logger.warning(
                    f"Dataset ID: {dataset_master.dataset_id} DQM check : {dqm_detail.qc_type} has some failed records of {failure_count} out of {total_count}."
                )
                return null_check_dqm_
            elif dqm_detail.criticality == "NC":
                # As its NC no exception is raised and only passed records are saved.
                orch_process.insert_log_dqm(
                    log_dqm=logDqmDtl(
                        process_id=dataset_master.dataset_id,
                        dataset_id=dataset_master.dataset_id,
                        batch_id=batch_id,
                        source_file=source_file,
                        column_name=dqm_detail.column_name,
                        qc_type=dqm_detail.qc_type,
                        qc_param=dqm_detail.qc_param,
                        qc_filter=dqm_detail.qc_param,
                        criticality=dqm_detail.criticality,
                        criticality_threshold_pct=dqm_detail.criticality_threshold_pct,
                        error_count=failure_count,
                        error_pct=failure_percentage,
                        status="SUCCEEDED",
                        dqm_start_time=start_time,
                        dqm_end_time=datetime.now(),
                    )
                )
                logger.warning(
                    f"Dataset ID: {dataset_master.dataset_id} DQM check : {dqm_detail.qc_type} has some failed records of {failure_count} out of {total_count}."
                )
                return null_check_dqm_

        else:
            orch_process.insert_log_dqm(
                log_dqm=logDqmDtl(
                    process_id=dataset_master.dataset_id,
                    dataset_id=dataset_master.dataset_id,
                    batch_id=batch_id,
                    source_file=source_file,
                    column_name=dqm_detail.column_name,
                    qc_type=dqm_detail.qc_type,
                    qc_param=dqm_detail.qc_param,
                    qc_filter=dqm_detail.qc_param,
                    criticality=dqm_detail.criticality,
                    criticality_threshold_pct=dqm_detail.criticality_threshold_pct,
                    error_count=0,
                    error_pct=failure_percentage,
                    status="SUCCEEDED",
                    dqm_start_time=start_time,
                    dqm_end_time=datetime.now(),
                )
            )
            logger.info(
                f"Dataset ID: {dataset_master.dataset_id} DQM check : {dqm_detail.qc_type} Has passed."
            )
            return null_check_dqm_

    def unique_dqm(
        self,
        df: polars.DataFrame,
        batch_id: int,
        source_file: str,
        dqm_detail: ctlDqmMasterDtl,
        dataset_master: ctlDatasetMaster,
        orch_process: OrchestrationProcess,
    ) -> polars.DataFrame:
        """
        Check that all values in a column (or set of columns) are unique.

        Uses `.unique()` to identify duplicates and logs them if criticality threshold is crossed.

        Args:
            df (polars.DataFrame): Input DataFrame to validate.
            batch_id (int): Batch ID associated with this data.
            source_file (str): Source file name for logging purposes.
            dqm_detail (ctlDqmMasterDtl): DQM rule definition object.
            dataset_master (ctlDatasetMaster): Dataset metadata object.
            orch_process (OrchestrationProcess): Instance used for logging.

        Returns:
            polars.DataFrame: DataFrame with only unique values.

        Raises:
            Exception: If duplicate values exceed criticality threshold and marked 'C'.
        """

        start_time = datetime.now()
        total_count = len(df)

        unique_dqm_df = df.unique(subset=dqm_detail.column_name.split(","))

        if dqm_detail.qc_filter:
            filter_cols = dqm_detail.qc_filter.split(",")
            filter_cols = " AND ".join(filter_cols)

            unique_dqm_df = unique_dqm_df.filter(
                polars.col(dqm_detail.column_name)
            ).sql(f"SELECT * FROM self WHERE {filter_cols}")

        failure_count = total_count - len(unique_dqm_df)

        if failure_count != 0:
            failure_percentage = (failure_count / total_count) * 100
            failed_records = df.join(unique_dqm_df, on=df.columns, how="anti")

            if (dqm_detail.criticality == "C") and (
                failure_percentage >= dqm_detail.criticality_threshold_pct
            ):
                orch_process.insert_log_dqm(
                    log_dqm=logDqmDtl(
                        process_id=dataset_master.dataset_id,
                        dataset_id=dataset_master.dataset_id,
                        batch_id=batch_id,
                        source_file=source_file,
                        column_name=dqm_detail.column_name,
                        qc_type=dqm_detail.qc_type,
                        qc_param=dqm_detail.qc_param,
                        qc_filter=dqm_detail.qc_param,
                        criticality=dqm_detail.criticality,
                        criticality_threshold_pct=dqm_detail.criticality_threshold_pct,
                        error_count=failure_count,
                        error_pct=failure_percentage,
                        status="FAILED",
                        dqm_start_time=start_time,
                        dqm_end_time=datetime.now(),
                    )
                )
                logger.error(
                    f"Dataset ID: {dataset_master.dataset_id} DQM check Failed For: {dqm_detail.qc_type} as it crossed Criticality Threshold {failure_percentage}%."
                )
                raise Exception(
                    f"DQM Checks failed for dataset ID: {dataset_master.dataset_id} for {dqm_detail.qc_type}-Check as it crossed Criticality Threshold {failure_percentage}%."
                )
            elif (dqm_detail.criticality == "C") and (
                failure_percentage <= dqm_detail.criticality_threshold_pct
            ):
                # Write Passed Records only
                orch_process.insert_log_dqm(
                    log_dqm=logDqmDtl(
                        process_id=dataset_master.dataset_id,
                        dataset_id=dataset_master.dataset_id,
                        batch_id=batch_id,
                        source_file=source_file,
                        column_name=dqm_detail.column_name,
                        qc_type=dqm_detail.qc_type,
                        qc_param=dqm_detail.qc_param,
                        qc_filter=dqm_detail.qc_param,
                        criticality=dqm_detail.criticality,
                        criticality_threshold_pct=dqm_detail.criticality_threshold_pct,
                        error_count=failure_count,
                        error_pct=failure_percentage,
                        status="SUCCEEDED",
                        dqm_start_time=start_time,
                        dqm_end_time=datetime.now(),
                    )
                )
                logger.warning(
                    f"Dataset ID: {dataset_master.dataset_id} DQM check : {dqm_detail.qc_type} has some failed records of {failure_count} out of {total_count}."
                )
                return unique_dqm_df
            elif dqm_detail.criticality == "NC":
                # As its NC no exception is raised and only passed records are saved.
                orch_process.insert_log_dqm(
                    log_dqm=logDqmDtl(
                        process_id=dataset_master.dataset_id,
                        dataset_id=dataset_master.dataset_id,
                        batch_id=batch_id,
                        source_file=source_file,
                        column_name=dqm_detail.column_name,
                        qc_type=dqm_detail.qc_type,
                        qc_param=dqm_detail.qc_param,
                        qc_filter=dqm_detail.qc_param,
                        criticality=dqm_detail.criticality,
                        criticality_threshold_pct=dqm_detail.criticality_threshold_pct,
                        error_count=failure_count,
                        error_pct=failure_percentage,
                        status="SUCCEEDED",
                        dqm_start_time=start_time,
                        dqm_end_time=datetime.now(),
                    )
                )
                logger.warning(
                    f"Dataset ID: {dataset_master.dataset_id} DQM check : {dqm_detail.qc_type} has some failed records of {failure_count} out of {total_count}."
                )
                return unique_dqm_df

        else:
            orch_process.insert_log_dqm(
                log_dqm=logDqmDtl(
                    process_id=dataset_master.dataset_id,
                    dataset_id=dataset_master.dataset_id,
                    batch_id=batch_id,
                    source_file=source_file,
                    column_name=dqm_detail.column_name,
                    qc_type=dqm_detail.qc_type,
                    qc_param=dqm_detail.qc_param,
                    qc_filter=dqm_detail.qc_param,
                    criticality=dqm_detail.criticality,
                    criticality_threshold_pct=dqm_detail.criticality_threshold_pct,
                    error_count=failure_count,
                    error_pct=0,
                    status="SUCCEEDED",
                    dqm_start_time=start_time,
                    dqm_end_time=datetime.now(),
                )
            )
            logger.info(
                f"Dataset ID: {dataset_master.dataset_id} DQM check : {dqm_detail.qc_type} Has passed."
            )
            return unique_dqm_df

    def length_dqm_check(
        self,
        df: polars.DataFrame,
        batch_id: int,
        source_file: str,
        dqm_detail: ctlDqmMasterDtl,
        dataset_master: ctlDatasetMaster,
        orch_process: OrchestrationProcess,
    ) -> polars.DataFrame:
        """
        Validate string length against an expected expression.

        Supports conditions like ">", "<", "==", etc., applied to string length.

        Args:
            df (polars.DataFrame): Input DataFrame to validate.
            batch_id (int): Batch ID associated with this data.
            source_file (str): Source file name for logging purposes.
            dqm_detail (ctlDqmMasterDtl): DQM rule definition object.
            dataset_master (ctlDatasetMaster): Dataset metadata object.
            orch_process (OrchestrationProcess): Instance used for logging.

        Returns:
            polars.DataFrame: DataFrame with only valid-length strings.

        Raises:
            Exception: If invalid lengths exceed criticality threshold and marked 'C'.
        """

        start_time = datetime.now()
        total_count = len(df)

        parsed_qc_param = json_loads(dqm_detail.qc_param)
        param_exp = parsed_qc_param["expression"]
        param_value = parsed_qc_param["value"]

        length_validation_df = df.sql(
            f"SELECT * FROM self WHERE length({dqm_detail.column_name}) {param_exp} {param_value}"
        )

        if dqm_detail.qc_filter:
            filter_cols = dqm_detail.qc_filter.split(",")
            filter_cols = " AND ".join(filter_cols)

            length_validation_df = length_validation_df.filter(
                polars.col(dqm_detail.column_name)
            ).sql(f"SELECT * FROM self WHERE {filter_cols}")

        failure_count = total_count - len(length_validation_df)

        if failure_count != 0:
            failure_percentage = (failure_count / total_count) * 100
            failed_records = df.join(length_validation_df, on=df.columns, how="anti")

            if (dqm_detail.criticality == "C") and (
                failure_percentage >= dqm_detail.criticality_threshold_pct
            ):
                orch_process.insert_log_dqm(
                    log_dqm=logDqmDtl(
                        process_id=dataset_master.dataset_id,
                        dataset_id=dataset_master.dataset_id,
                        batch_id=batch_id,
                        source_file=source_file,
                        column_name=dqm_detail.column_name,
                        qc_type=dqm_detail.qc_type,
                        qc_param=dqm_detail.qc_param,
                        qc_filter=dqm_detail.qc_param,
                        criticality=dqm_detail.criticality,
                        criticality_threshold_pct=dqm_detail.criticality_threshold_pct,
                        error_count=failure_count,
                        error_pct=failure_percentage,
                        status="FAILED",
                        dqm_start_time=start_time,
                        dqm_end_time=datetime.now(),
                    )
                )
                logger.error(
                    f"Dataset ID: {dataset_master.dataset_id} DQM check Failed For: {dqm_detail.qc_type} as it crossed Criticality Threshold {failure_percentage}%."
                )
                raise Exception(
                    f"DQM Checks failed for dataset ID: {dataset_master.dataset_id} for {dqm_detail.qc_type}-Check as it crossed Criticality Threshold {failure_percentage}%."
                )
            elif (dqm_detail.criticality == "C") and (
                failure_percentage <= dqm_detail.criticality_threshold_pct
            ):
                # Write Passed Records only
                orch_process.insert_log_dqm(
                    log_dqm=logDqmDtl(
                        process_id=dataset_master.dataset_id,
                        dataset_id=dataset_master.dataset_id,
                        batch_id=batch_id,
                        source_file=source_file,
                        column_name=dqm_detail.column_name,
                        qc_type=dqm_detail.qc_type,
                        qc_param=dqm_detail.qc_param,
                        qc_filter=dqm_detail.qc_param,
                        criticality=dqm_detail.criticality,
                        criticality_threshold_pct=dqm_detail.criticality_threshold_pct,
                        error_count=failure_count,
                        error_pct=failure_percentage,
                        status="SUCCEEDED",
                        dqm_start_time=start_time,
                        dqm_end_time=datetime.now(),
                    )
                )
                logger.warning(
                    f"Dataset ID: {dataset_master.dataset_id} DQM check : {dqm_detail.qc_type} has some failed records of {failure_count} out of {total_count}."
                )
                return length_validation_df
            elif dqm_detail.criticality == "NC":
                # As its NC no exception is raised and only passed records are saved.
                orch_process.insert_log_dqm(
                    log_dqm=logDqmDtl(
                        process_id=dataset_master.dataset_id,
                        dataset_id=dataset_master.dataset_id,
                        batch_id=batch_id,
                        source_file=source_file,
                        column_name=dqm_detail.column_name,
                        qc_type=dqm_detail.qc_type,
                        qc_param=dqm_detail.qc_param,
                        qc_filter=dqm_detail.qc_param,
                        criticality=dqm_detail.criticality,
                        criticality_threshold_pct=dqm_detail.criticality_threshold_pct,
                        error_count=failure_count,
                        error_pct=failure_percentage,
                        status="SUCCEEDED",
                        dqm_start_time=start_time,
                        dqm_end_time=datetime.now(),
                    )
                )
                logger.warning(
                    f"Dataset ID: {dataset_master.dataset_id} DQM check : {dqm_detail.qc_type} has some failed records of {failure_count} out of {total_count}."
                )
                return length_validation_df
        else:
            orch_process.insert_log_dqm(
                log_dqm=logDqmDtl(
                    process_id=dataset_master.dataset_id,
                    dataset_id=dataset_master.dataset_id,
                    batch_id=batch_id,
                    source_file=source_file,
                    column_name=dqm_detail.column_name,
                    qc_type=dqm_detail.qc_type,
                    qc_param=dqm_detail.qc_param,
                    qc_filter=dqm_detail.qc_param,
                    criticality=dqm_detail.criticality,
                    criticality_threshold_pct=dqm_detail.criticality_threshold_pct,
                    error_count=failure_count,
                    error_pct=0,
                    status="SUCCEEDED",
                    dqm_start_time=start_time,
                    dqm_end_time=datetime.now(),
                )
            )
            logger.info(
                f"Dataset ID: {dataset_master.dataset_id} DQM check : {dqm_detail.qc_type} Has passed."
            )
            return length_validation_df

    def date_dqm_check(
        self,
        df: polars.DataFrame,
        batch_id: int,
        source_file: str,
        dqm_detail: ctlDqmMasterDtl,
        dataset_master: ctlDatasetMaster,
        orch_process: OrchestrationProcess,
    ) -> polars.DataFrame:
        """
        Validate column values match expected date format using regex patterns.

        Args:
            df (polars.DataFrame): Input DataFrame to validate.
            batch_id (int): Batch ID associated with this data.
            source_file (str): Source file name for logging purposes.
            dqm_detail (ctlDqmMasterDtl): DQM rule definition object.
            dataset_master (ctlDatasetMaster): Dataset metadata object.
            orch_process (OrchestrationProcess): Instance used for logging.

        Returns:
            polars.DataFrame: DataFrame with only valid date-formatted values.

        Raises:
            Exception: If invalid dates exceed criticality threshold and marked 'C'.
        """

        start_time = datetime.now()
        total_count = len(df)

        date_regex_pattern = get_date_regex(qc_param=dqm_detail.qc_param)

        date_dqm_df = df.filter(
            polars.col(dqm_detail.column_name).str.contains(date_regex_pattern)
        )

        if dqm_detail.qc_filter:
            filter_cols = dqm_detail.qc_filter.split(",")
            filter_cols = " AND ".join(filter_cols)

            date_dqm_df = date_dqm_df.filter(polars.col(dqm_detail.column_name)).sql(
                f"SELECT * FROM self WHERE {filter_cols}"
            )

        failure_count = total_count - len(date_dqm_df)

        if failure_count != 0:
            failure_percentage = (failure_count / total_count) * 100
            failed_records = df.join(date_dqm_df, on=df.columns, how="anti")

            if (dqm_detail.criticality == "C") and (
                failure_percentage >= dqm_detail.criticality_threshold_pct
            ):
                orch_process.insert_log_dqm(
                    log_dqm=logDqmDtl(
                        process_id=dataset_master.dataset_id,
                        dataset_id=dataset_master.dataset_id,
                        batch_id=batch_id,
                        source_file=source_file,
                        column_name=dqm_detail.column_name,
                        qc_type=dqm_detail.qc_type,
                        qc_param=dqm_detail.qc_param,
                        qc_filter=dqm_detail.qc_param,
                        criticality=dqm_detail.criticality,
                        criticality_threshold_pct=dqm_detail.criticality_threshold_pct,
                        error_count=failure_count,
                        error_pct=failure_percentage,
                        status="FAILED",
                        dqm_start_time=start_time,
                        dqm_end_time=datetime.now(),
                    )
                )
                logger.error(
                    f"Dataset ID: {dataset_master.dataset_id} DQM check Failed For: {dqm_detail.qc_type} as it crossed Criticality Threshold {failure_percentage}%."
                )
                raise Exception(
                    f"DQM Checks failed for dataset ID: {dataset_master.dataset_id} for {dqm_detail.qc_type}-Check as it crossed Criticality Threshold {failure_percentage}%."
                )
            elif (dqm_detail.criticality == "C") and (
                failure_percentage <= dqm_detail.criticality_threshold_pct
            ):
                # Write Passed Records only
                orch_process.insert_log_dqm(
                    log_dqm=logDqmDtl(
                        process_id=dataset_master.dataset_id,
                        dataset_id=dataset_master.dataset_id,
                        batch_id=batch_id,
                        source_file=source_file,
                        column_name=dqm_detail.column_name,
                        qc_type=dqm_detail.qc_type,
                        qc_param=dqm_detail.qc_param,
                        qc_filter=dqm_detail.qc_param,
                        criticality=dqm_detail.criticality,
                        criticality_threshold_pct=dqm_detail.criticality_threshold_pct,
                        error_count=failure_count,
                        error_pct=failure_percentage,
                        status="SUCCEEDED",
                        dqm_start_time=start_time,
                        dqm_end_time=datetime.now(),
                    )
                )
                logger.warning(
                    f"Dataset ID: {dataset_master.dataset_id} DQM check : {dqm_detail.qc_type} has some failed records of {failure_count} out of {total_count}."
                )
                return date_dqm_df
            elif dqm_detail.criticality == "NC":
                # As its NC no exception is raised and only passed records are saved.
                orch_process.insert_log_dqm(
                    log_dqm=logDqmDtl(
                        process_id=dataset_master.dataset_id,
                        dataset_id=dataset_master.dataset_id,
                        batch_id=batch_id,
                        source_file=source_file,
                        column_name=dqm_detail.column_name,
                        qc_type=dqm_detail.qc_type,
                        qc_param=dqm_detail.qc_param,
                        qc_filter=dqm_detail.qc_param,
                        criticality=dqm_detail.criticality,
                        criticality_threshold_pct=dqm_detail.criticality_threshold_pct,
                        error_count=failure_count,
                        error_pct=failure_percentage,
                        status="SUCCEEDED",
                        dqm_start_time=start_time,
                        dqm_end_time=datetime.now(),
                    )
                )
                logger.warning(
                    f"Dataset ID: {dataset_master.dataset_id} DQM check : {dqm_detail.qc_type} has some failed records of {failure_count} out of {total_count}."
                )
                return date_dqm_df
        else:
            orch_process.insert_log_dqm(
                log_dqm=logDqmDtl(
                    process_id=dataset_master.dataset_id,
                    dataset_id=dataset_master.dataset_id,
                    batch_id=batch_id,
                    source_file=source_file,
                    column_name=dqm_detail.column_name,
                    qc_type=dqm_detail.qc_type,
                    qc_param=dqm_detail.qc_param,
                    qc_filter=dqm_detail.qc_param,
                    criticality=dqm_detail.criticality,
                    criticality_threshold_pct=dqm_detail.criticality_threshold_pct,
                    error_count=failure_count,
                    error_pct=0,
                    status="SUCCEEDED",
                    dqm_start_time=start_time,
                    dqm_end_time=datetime.now(),
                )
            )
            logger.info(
                f"Dataset ID: {dataset_master.dataset_id} DQM check : {dqm_detail.qc_type} Has passed."
            )
            return date_dqm_df

    def integer_dqm_check(
        self,
        df: polars.DataFrame,
        batch_id: int,
        source_file: str,
        dqm_detail: ctlDqmMasterDtl,
        dataset_master: ctlDatasetMaster,
        orch_process: OrchestrationProcess,
    ) -> polars.DataFrame:
        r"""
        Validate column values are integers.

        Uses regex pattern `(^\d+$)` to identify valid integers.

        Args:
            df (polars.DataFrame): Input DataFrame to validate.
            batch_id (int): Batch ID associated with this data.
            source_file (str): Source file name for logging purposes.
            dqm_detail (ctlDqmMasterDtl): DQM rule definition object.
            dataset_master (ctlDatasetMaster): Dataset metadata object.
            orch_process (OrchestrationProcess): Instance used for logging.

        Returns:
            polars.DataFrame: DataFrame with only valid integer values.

        Raises:
            Exception: If invalid values exceed criticality threshold and marked 'C'.
        """

        start_time = datetime.now()
        total_count = len(df)

        integer_dqm_df = df.filter(
            polars.col(dqm_detail.column_name).str.contains(r"(^-?\d+$)")
        )

        if dqm_detail.qc_filter:
            filter_cols = dqm_detail.qc_filter.split(",")
            filter_cols = " AND ".join(filter_cols)

            integer_dqm_df = integer_dqm_df.filter(
                polars.col(dqm_detail.column_name)
            ).sql(f"SELECT * FROM self WHERE {filter_cols}")

        failure_count = total_count - len(integer_dqm_df)

        if failure_count != 0:
            failure_percentage = (failure_count / total_count) * 100
            failed_records = df.join(integer_dqm_df, on=df.columns, how="anti")

            if (dqm_detail.criticality == "C") and (
                failure_percentage >= dqm_detail.criticality_threshold_pct
            ):
                orch_process.insert_log_dqm(
                    log_dqm=logDqmDtl(
                        process_id=dataset_master.dataset_id,
                        dataset_id=dataset_master.dataset_id,
                        batch_id=batch_id,
                        source_file=source_file,
                        column_name=dqm_detail.column_name,
                        qc_type=dqm_detail.qc_type,
                        qc_param=dqm_detail.qc_param,
                        qc_filter=dqm_detail.qc_param,
                        criticality=dqm_detail.criticality,
                        criticality_threshold_pct=dqm_detail.criticality_threshold_pct,
                        error_count=failure_count,
                        error_pct=failure_percentage,
                        status="FAILED",
                        dqm_start_time=start_time,
                        dqm_end_time=datetime.now(),
                    )
                )
                logger.error(
                    f"Dataset ID: {dataset_master.dataset_id} DQM check Failed For: {dqm_detail.qc_type} as it crossed Criticality Threshold {failure_percentage}%."
                )
                raise Exception(
                    f"DQM Checks failed for dataset ID: {dataset_master.dataset_id} for {dqm_detail.qc_type}-Check as it crossed Criticality Threshold {failure_percentage}%."
                )
            elif (dqm_detail.criticality == "C") and (
                failure_percentage <= dqm_detail.criticality_threshold_pct
            ):
                # Write Passed Records only
                orch_process.insert_log_dqm(
                    log_dqm=logDqmDtl(
                        process_id=dataset_master.dataset_id,
                        dataset_id=dataset_master.dataset_id,
                        batch_id=batch_id,
                        source_file=source_file,
                        column_name=dqm_detail.column_name,
                        qc_type=dqm_detail.qc_type,
                        qc_param=dqm_detail.qc_param,
                        qc_filter=dqm_detail.qc_param,
                        criticality=dqm_detail.criticality,
                        criticality_threshold_pct=dqm_detail.criticality_threshold_pct,
                        error_count=failure_count,
                        error_pct=failure_percentage,
                        status="SUCCEEDED",
                        dqm_start_time=start_time,
                        dqm_end_time=datetime.now(),
                    )
                )
                logger.warning(
                    f"Dataset ID: {dataset_master.dataset_id} DQM check : {dqm_detail.qc_type} has some failed records of {failure_count} out of {total_count}."
                )
                return integer_dqm_df
            elif dqm_detail.criticality == "NC":
                # As its NC no exception is raised and only passed records are saved.
                orch_process.insert_log_dqm(
                    log_dqm=logDqmDtl(
                        process_id=dataset_master.dataset_id,
                        dataset_id=dataset_master.dataset_id,
                        batch_id=batch_id,
                        source_file=source_file,
                        column_name=dqm_detail.column_name,
                        qc_type=dqm_detail.qc_type,
                        qc_param=dqm_detail.qc_param,
                        qc_filter=dqm_detail.qc_param,
                        criticality=dqm_detail.criticality,
                        criticality_threshold_pct=dqm_detail.criticality_threshold_pct,
                        error_count=failure_count,
                        error_pct=failure_percentage,
                        status="SUCCEEDED",
                        dqm_start_time=start_time,
                        dqm_end_time=datetime.now(),
                    )
                )
                logger.warning(
                    f"Dataset ID: {dataset_master.dataset_id} DQM check : {dqm_detail.qc_type} has some failed records of {failure_count} out of {total_count}."
                )
                return integer_dqm_df
        else:
            orch_process.insert_log_dqm(
                log_dqm=logDqmDtl(
                    process_id=dataset_master.dataset_id,
                    dataset_id=dataset_master.dataset_id,
                    batch_id=batch_id,
                    source_file=source_file,
                    column_name=dqm_detail.column_name,
                    qc_type=dqm_detail.qc_type,
                    qc_param=dqm_detail.qc_param,
                    qc_filter=dqm_detail.qc_param,
                    criticality=dqm_detail.criticality,
                    criticality_threshold_pct=dqm_detail.criticality_threshold_pct,
                    error_count=failure_count,
                    error_pct=0,
                    status="SUCCEEDED",
                    dqm_start_time=start_time,
                    dqm_end_time=datetime.now(),
                )
            )
            logger.info(
                f"Dataset ID: {dataset_master.dataset_id} DQM check : {dqm_detail.qc_type} Has passed."
            )
            return integer_dqm_df

    def decimal_dqm_check(
        self,
        df: polars.DataFrame,
        batch_id: int,
        source_file: str,
        dqm_detail: ctlDqmMasterDtl,
        dataset_master: ctlDatasetMaster,
        orch_process: OrchestrationProcess,
    ) -> polars.DataFrame:
        r"""
        Validate column values are numeric (including decimals).

        Uses regex pattern `(^\d*[.]?\d+$)` to identify valid numeric values.

        Args:
            df (polars.DataFrame): Input DataFrame to validate.
            batch_id (int): Batch ID associated with this data.
            source_file (str): Source file name for logging purposes.
            dqm_detail (ctlDqmMasterDtl): DQM rule definition object.
            dataset_master (ctlDatasetMaster): Dataset metadata object.
            orch_process (OrchestrationProcess): Instance used for logging.

        Returns:
            polars.DataFrame: DataFrame with only valid decimal values.

        Raises:
            Exception: If invalid values exceed criticality threshold and marked 'C'.
        """

        start_time = datetime.now()
        total_count = len(df)

        decimal_dqm_df = df.filter(
            polars.col(dqm_detail.column_name).str.contains(r"(^-?\d+$)")
        )

        if dqm_detail.qc_filter:
            filter_cols = dqm_detail.qc_filter.split(",")
            filter_cols = " AND ".join(filter_cols)

            decimal_dqm_df = decimal_dqm_df.filter(
                polars.col(dqm_detail.column_name)
            ).sql(f"SELECT * FROM self WHERE {filter_cols}")

        failure_count = total_count - len(decimal_dqm_df)

        if failure_count != 0:
            failure_percentage = (failure_count / total_count) * 100
            failed_records = df.join(decimal_dqm_df, on=df.columns, how="anti")

            if (dqm_detail.criticality == "C") and (
                failure_percentage >= dqm_detail.criticality_threshold_pct
            ):
                orch_process.insert_log_dqm(
                    log_dqm=logDqmDtl(
                        process_id=dataset_master.dataset_id,
                        dataset_id=dataset_master.dataset_id,
                        batch_id=batch_id,
                        source_file=source_file,
                        column_name=dqm_detail.column_name,
                        qc_type=dqm_detail.qc_type,
                        qc_param=dqm_detail.qc_param,
                        qc_filter=dqm_detail.qc_param,
                        criticality=dqm_detail.criticality,
                        criticality_threshold_pct=dqm_detail.criticality_threshold_pct,
                        error_count=failure_count,
                        error_pct=failure_percentage,
                        status="FAILED",
                        dqm_start_time=start_time,
                        dqm_end_time=datetime.now(),
                    )
                )
                logger.error(
                    f"Dataset ID: {dataset_master.dataset_id} DQM check Failed For: {dqm_detail.qc_type} as it crossed Criticality Threshold {failure_percentage}%."
                )
                raise Exception(
                    f"DQM Checks failed for dataset ID: {dataset_master.dataset_id} for {dqm_detail.qc_type}-Check as it crossed Criticality Threshold {failure_percentage}%."
                )
            elif (dqm_detail.criticality == "C") and (
                failure_percentage <= dqm_detail.criticality_threshold_pct
            ):
                # Write Passed Records only
                orch_process.insert_log_dqm(
                    log_dqm=logDqmDtl(
                        process_id=dataset_master.dataset_id,
                        dataset_id=dataset_master.dataset_id,
                        batch_id=batch_id,
                        source_file=source_file,
                        column_name=dqm_detail.column_name,
                        qc_type=dqm_detail.qc_type,
                        qc_param=dqm_detail.qc_param,
                        qc_filter=dqm_detail.qc_param,
                        criticality=dqm_detail.criticality,
                        criticality_threshold_pct=dqm_detail.criticality_threshold_pct,
                        error_count=failure_count,
                        error_pct=failure_percentage,
                        status="SUCCEEDED",
                        dqm_start_time=start_time,
                        dqm_end_time=datetime.now(),
                    )
                )
                logger.warning(
                    f"Dataset ID: {dataset_master.dataset_id} DQM check : {dqm_detail.qc_type} has some failed records of {failure_count} out of {total_count}."
                )
                return decimal_dqm_df
            elif dqm_detail.criticality == "NC":
                # As its NC no exception is raised and only passed records are saved.
                orch_process.insert_log_dqm(
                    log_dqm=logDqmDtl(
                        process_id=dataset_master.dataset_id,
                        dataset_id=dataset_master.dataset_id,
                        batch_id=batch_id,
                        source_file=source_file,
                        column_name=dqm_detail.column_name,
                        qc_type=dqm_detail.qc_type,
                        qc_param=dqm_detail.qc_param,
                        qc_filter=dqm_detail.qc_param,
                        criticality=dqm_detail.criticality,
                        criticality_threshold_pct=dqm_detail.criticality_threshold_pct,
                        error_count=failure_count,
                        error_pct=failure_percentage,
                        status="SUCCEEDED",
                        dqm_start_time=start_time,
                        dqm_end_time=datetime.now(),
                    )
                )
                logger.warning(
                    f"Dataset ID: {dataset_master.dataset_id} DQM check : {dqm_detail.qc_type} has some failed records of {failure_count} out of {total_count}."
                )
                return decimal_dqm_df
        else:
            orch_process.insert_log_dqm(
                log_dqm=logDqmDtl(
                    process_id=dataset_master.dataset_id,
                    dataset_id=dataset_master.dataset_id,
                    batch_id=batch_id,
                    source_file=source_file,
                    column_name=dqm_detail.column_name,
                    qc_type=dqm_detail.qc_type,
                    qc_param=dqm_detail.qc_param,
                    qc_filter=dqm_detail.qc_param,
                    criticality=dqm_detail.criticality,
                    criticality_threshold_pct=dqm_detail.criticality_threshold_pct,
                    error_count=failure_count,
                    error_pct=0,
                    status="SUCCEEDED",
                    dqm_start_time=start_time,
                    dqm_end_time=datetime.now(),
                )
            )
            logger.info(
                f"Dataset ID: {dataset_master.dataset_id} DQM check : {dqm_detail.qc_type} Has passed."
            )
            return decimal_dqm_df

    def domain_dqm_check(
        self,
        df: polars.DataFrame,
        batch_id: int,
        source_file: str,
        dqm_detail: ctlDqmMasterDtl,
        dataset_master: ctlDatasetMaster,
        orch_process: OrchestrationProcess,
    ) -> polars.DataFrame:
        """
        Validate column values fall within a defined domain.

        Accepts comma-separated allowed values from `qc_param`.

        Args:
            df (polars.DataFrame): Input DataFrame to validate.
            batch_id (int): Batch ID associated with this data.
            source_file (str): Source file name for logging purposes.
            dqm_detail (ctlDqmMasterDtl): DQM rule definition object.
            dataset_master (ctlDatasetMaster): Dataset metadata object.
            orch_process (OrchestrationProcess): Instance used for logging.

        Returns:
            polars.DataFrame: DataFrame with only valid domain values.

        Raises:
            Exception: If invalid values exceed criticality threshold and marked 'C'.
        """

        start_time = datetime.now()
        total_count = len(df)

        domain_dqm_df = df.filter(
            polars.col(dqm_detail.column_name).is_in(dqm_detail.qc_param.split(","))
        )

        if dqm_detail.qc_filter:
            filter_cols = dqm_detail.qc_filter.split(",")
            filter_cols = " AND ".join(filter_cols)

            domain_dqm_df = domain_dqm_df.filter(
                polars.col(dqm_detail.column_name)
            ).sql(f"SELECT * FROM self WHERE {filter_cols}")

        failure_count = total_count - len(domain_dqm_df)

        if failure_count != 0:
            failure_percentage = (failure_count / total_count) * 100
            failed_records = df.join(domain_dqm_df, on=df.columns, how="anti")

            if (dqm_detail.criticality == "C") and (
                failure_percentage >= dqm_detail.criticality_threshold_pct
            ):
                orch_process.insert_log_dqm(
                    log_dqm=logDqmDtl(
                        process_id=dataset_master.dataset_id,
                        dataset_id=dataset_master.dataset_id,
                        batch_id=batch_id,
                        source_file=source_file,
                        column_name=dqm_detail.column_name,
                        qc_type=dqm_detail.qc_type,
                        qc_param=dqm_detail.qc_param,
                        qc_filter=dqm_detail.qc_param,
                        criticality=dqm_detail.criticality,
                        criticality_threshold_pct=dqm_detail.criticality_threshold_pct,
                        error_count=failure_count,
                        error_pct=failure_percentage,
                        status="FAILED",
                        dqm_start_time=start_time,
                        dqm_end_time=datetime.now(),
                    )
                )
                logger.error(
                    f"Dataset ID: {dataset_master.dataset_id} DQM check Failed For: {dqm_detail.qc_type} as it crossed Criticality Threshold {failure_percentage}%."
                )
                raise Exception(
                    f"DQM Checks failed for dataset ID: {dataset_master.dataset_id} for {dqm_detail.qc_type}-Check as it crossed Criticality Threshold {failure_percentage}%."
                )
            elif (dqm_detail.criticality == "C") and (
                failure_percentage <= dqm_detail.criticality_threshold_pct
            ):
                # Write Passed Records only
                orch_process.insert_log_dqm(
                    log_dqm=logDqmDtl(
                        process_id=dataset_master.dataset_id,
                        dataset_id=dataset_master.dataset_id,
                        batch_id=batch_id,
                        source_file=source_file,
                        column_name=dqm_detail.column_name,
                        qc_type=dqm_detail.qc_type,
                        qc_param=dqm_detail.qc_param,
                        qc_filter=dqm_detail.qc_param,
                        criticality=dqm_detail.criticality,
                        criticality_threshold_pct=dqm_detail.criticality_threshold_pct,
                        error_count=failure_count,
                        error_pct=failure_percentage,
                        status="SUCCEEDED",
                        dqm_start_time=start_time,
                        dqm_end_time=datetime.now(),
                    )
                )
                logger.warning(
                    f"Dataset ID: {dataset_master.dataset_id} DQM check : {dqm_detail.qc_type} has some failed records of {failure_count} out of {total_count}."
                )
                return domain_dqm_df
            elif dqm_detail.criticality == "NC":
                # As its NC no exception is raised and only passed records are saved.
                orch_process.insert_log_dqm(
                    log_dqm=logDqmDtl(
                        process_id=dataset_master.dataset_id,
                        dataset_id=dataset_master.dataset_id,
                        batch_id=batch_id,
                        source_file=source_file,
                        column_name=dqm_detail.column_name,
                        qc_type=dqm_detail.qc_type,
                        qc_param=dqm_detail.qc_param,
                        qc_filter=dqm_detail.qc_param,
                        criticality=dqm_detail.criticality,
                        criticality_threshold_pct=dqm_detail.criticality_threshold_pct,
                        error_count=failure_count,
                        error_pct=failure_percentage,
                        status="SUCCEEDED",
                        dqm_start_time=start_time,
                        dqm_end_time=datetime.now(),
                    )
                )
                logger.warning(
                    f"Dataset ID: {dataset_master.dataset_id} DQM check : {dqm_detail.qc_type} has some failed records of {failure_count} out of {total_count}."
                )
                return domain_dqm_df
        else:
            orch_process.insert_log_dqm(
                log_dqm=logDqmDtl(
                    process_id=dataset_master.dataset_id,
                    dataset_id=dataset_master.dataset_id,
                    batch_id=batch_id,
                    source_file=source_file,
                    column_name=dqm_detail.column_name,
                    qc_type=dqm_detail.qc_type,
                    qc_param=dqm_detail.qc_param,
                    qc_filter=dqm_detail.qc_param,
                    criticality=dqm_detail.criticality,
                    criticality_threshold_pct=dqm_detail.criticality_threshold_pct,
                    error_count=failure_count,
                    error_pct=0,
                    status="SUCCEEDED",
                    dqm_start_time=start_time,
                    dqm_end_time=datetime.now(),
                )
            )
            logger.info(
                f"Dataset ID: {dataset_master.dataset_id} DQM check : {dqm_detail.qc_type} Has passed."
            )
            return domain_dqm_df

    def custom_dqm_check(
        self,
        df: polars.DataFrame,
        batch_id: int,
        source_file: str,
        dqm_detail: ctlDqmMasterDtl,
        dataset_master: ctlDatasetMaster,
        orch_process: OrchestrationProcess,
    ) -> polars.DataFrame:
        """
        Apply custom SQL-like filter to perform flexible quality checks.

        Args:
            df (polars.DataFrame): Input DataFrame to validate.
            batch_id (int): Batch ID associated with this data.
            source_file (str): Source file name for logging purposes.
            dqm_detail (ctlDqmMasterDtl): DQM rule definition object.
            dataset_master (ctlDatasetMaster): Dataset metadata object.
            orch_process (OrchestrationProcess): Instance used for logging.

        Returns:
            polars.DataFrame: DataFrame with only records passing the custom filter.

        Raises:
            Exception: If invalid values exceed criticality threshold and marked 'C'.
        """

        start_time = datetime.now()
        total_count = len(df)

        custom_dqm_df = df.filter(
            polars.col(dqm_detail.column_name).sql(dqm_detail.qc_param)
        )

        failure_count = total_count - len(custom_dqm_df)

        if failure_count != 0:
            failure_percentage = (failure_count / total_count) * 100
            failed_records = df.join(custom_dqm_df, on=df.columns, how="anti")

            if (dqm_detail.criticality == "C") and (
                failure_percentage >= dqm_detail.criticality_threshold_pct
            ):
                orch_process.insert_log_dqm(
                    log_dqm=logDqmDtl(
                        process_id=dataset_master.dataset_id,
                        dataset_id=dataset_master.dataset_id,
                        batch_id=batch_id,
                        source_file=source_file,
                        column_name=dqm_detail.column_name,
                        qc_type=dqm_detail.qc_type,
                        qc_param=dqm_detail.qc_param,
                        qc_filter=dqm_detail.qc_param,
                        criticality=dqm_detail.criticality,
                        criticality_threshold_pct=dqm_detail.criticality_threshold_pct,
                        error_count=failure_count,
                        error_pct=failure_percentage,
                        status="FAILED",
                        dqm_start_time=start_time,
                        dqm_end_time=datetime.now(),
                    )
                )
                logger.error(
                    f"Dataset ID: {dataset_master.dataset_id} DQM check Failed For: {dqm_detail.qc_type} as it crossed Criticality Threshold {failure_percentage}%."
                )
                raise Exception(
                    f"DQM Checks failed for dataset ID: {dataset_master.dataset_id} for {dqm_detail.qc_type}-Check as it crossed Criticality Threshold {failure_percentage}%."
                )
            elif (dqm_detail.criticality == "C") and (
                failure_percentage <= dqm_detail.criticality_threshold_pct
            ):
                # Write Passed Records only
                orch_process.insert_log_dqm(
                    log_dqm=logDqmDtl(
                        process_id=dataset_master.dataset_id,
                        dataset_id=dataset_master.dataset_id,
                        batch_id=batch_id,
                        source_file=source_file,
                        column_name=dqm_detail.column_name,
                        qc_type=dqm_detail.qc_type,
                        qc_param=dqm_detail.qc_param,
                        qc_filter=dqm_detail.qc_param,
                        criticality=dqm_detail.criticality,
                        criticality_threshold_pct=dqm_detail.criticality_threshold_pct,
                        error_count=failure_count,
                        error_pct=failure_percentage,
                        status="SUCCEEDED",
                        dqm_start_time=start_time,
                        dqm_end_time=datetime.now(),
                    )
                )
                logger.warning(
                    f"Dataset ID: {dataset_master.dataset_id} DQM check : {dqm_detail.qc_type} has some failed records of {failure_count} out of {total_count}."
                )
                return custom_dqm_df
            elif dqm_detail.criticality == "NC":
                # As its NC no exception is raised and only passed records are saved.
                orch_process.insert_log_dqm(
                    log_dqm=logDqmDtl(
                        process_id=dataset_master.dataset_id,
                        dataset_id=dataset_master.dataset_id,
                        batch_id=batch_id,
                        source_file=source_file,
                        column_name=dqm_detail.column_name,
                        qc_type=dqm_detail.qc_type,
                        qc_param=dqm_detail.qc_param,
                        qc_filter=dqm_detail.qc_param,
                        criticality=dqm_detail.criticality,
                        criticality_threshold_pct=dqm_detail.criticality_threshold_pct,
                        error_count=failure_count,
                        error_pct=failure_percentage,
                        status="SUCCEEDED",
                        dqm_start_time=start_time,
                        dqm_end_time=datetime.now(),
                    )
                )
                logger.warning(
                    f"Dataset ID: {dataset_master.dataset_id} DQM check : {dqm_detail.qc_type} has some failed records of {failure_count} out of {total_count}."
                )
                return custom_dqm_df
        else:
            orch_process.insert_log_dqm(
                log_dqm=logDqmDtl(
                    process_id=dataset_master.dataset_id,
                    dataset_id=dataset_master.dataset_id,
                    batch_id=batch_id,
                    source_file=source_file,
                    column_name=dqm_detail.column_name,
                    qc_type=dqm_detail.qc_type,
                    qc_param=dqm_detail.qc_param,
                    qc_filter=dqm_detail.qc_param,
                    criticality=dqm_detail.criticality,
                    criticality_threshold_pct=dqm_detail.criticality_threshold_pct,
                    error_count=failure_count,
                    error_pct=0,
                    status="SUCCEEDED",
                    dqm_start_time=start_time,
                    dqm_end_time=datetime.now(),
                )
            )
            logger.info(
                f"Dataset ID: {dataset_master.dataset_id} DQM check : {dqm_detail.qc_type} Has passed."
            )
            return custom_dqm_df

    def __init__(
        self,
        dqm_details: list[ctlDqmMasterDtl],
        dataset_master: ctlDatasetMaster,
    ):
        """
        Initialize and execute DQM checks for unprocessed transformation files.

        Reads transformation data from Delta, applies DQM rules, logs results,
        and writes clean data to publish layer.

        Args:
            dqm_details (list[ctlDqmMasterDtl]): List of DQM rule definitions.
            dataset_master (ctlDatasetMaster): Metadata about dataset and paths.

        Raises:
            Exception: If no unprocessed files found or any critical DQM check fails.
        """

        with OrchestrationProcess() as orch_process:
            unprocessed_files = orch_process.get_dqm_unprocessed_files(
                process_id=dataset_master.process_id,
                dataset_id=dataset_master.dataset_id,
            )

            if len(unprocessed_files) == 0:
                raise Exception(
                    f"No unprocess files found for Data Quality Checks for dataset id: {dataset_master.dataset_id}"
                )

            for unprocessed_file in unprocessed_files:
                data_standard_path = path_to_s3(
                    location=dataset_master.data_standardisation_location,
                    env=env,
                )
                compute_dqm_path = path_to_s3(
                    location=dataset_master.staging_location, env=env
                )

                if len(dqm_details) != 0:
                    # First read data standard data.
                    original_df = DeltaTableRead(
                        delta_path=data_standard_path["s3_location"],
                        batch_id=unprocessed_file.batch_id,
                    ).read()

                    dqm_check_df = original_df

                    for dqm_detail in dqm_details:
                        if dqm_detail.qc_type.lower() == "null":
                            dqm_check_df = self.null_check(
                                df=dqm_check_df,
                                batch_id=unprocessed_file.batch_id,
                                source_file=unprocessed_file.source_file,
                                dqm_detail=dqm_detail,
                                dataset_master=dataset_master,
                                orch_process=orch_process,
                            )

                        if dqm_detail.qc_type.lower() == "unique":
                            dqm_check_df = self.unique_dqm(
                                df=dqm_check_df,
                                batch_id=unprocessed_file.batch_id,
                                source_file=unprocessed_file.source_file,
                                dqm_detail=dqm_detail,
                                dataset_master=dataset_master,
                                orch_process=orch_process,
                            )

                        if dqm_detail.qc_type.lower() == "decimal":
                            dqm_check_df = self.decimal_dqm_check(
                                df=dqm_check_df,
                                batch_id=unprocessed_file.batch_id,
                                source_file=unprocessed_file.source_file,
                                dqm_detail=dqm_detail,
                                dataset_master=dataset_master,
                                orch_process=orch_process,
                            )

                        if dqm_detail.qc_type.lower() == "integer":
                            dqm_check_df = self.integer_dqm_check(
                                df=dqm_check_df,
                                batch_id=unprocessed_file.batch_id,
                                source_file=unprocessed_file.source_file,
                                dqm_detail=dqm_detail,
                                dataset_master=dataset_master,
                                orch_process=orch_process,
                            )

                        if dqm_detail.qc_type.lower() == "length":
                            dqm_check_df = self.length_dqm_check(
                                df=dqm_check_df,
                                batch_id=unprocessed_file.batch_id,
                                source_file=unprocessed_file.source_file,
                                dqm_detail=dqm_detail,
                                dataset_master=dataset_master,
                                orch_process=orch_process,
                            )

                        if dqm_detail.qc_type.lower() == "date":
                            dqm_check_df = self.date_dqm_check(
                                df=dqm_check_df,
                                batch_id=unprocessed_file.batch_id,
                                source_file=unprocessed_file.source_file,
                                dqm_detail=dqm_detail,
                                dataset_master=dataset_master,
                                orch_process=orch_process,
                            )

                        if dqm_detail.qc_type.lower() == "domain":
                            dqm_check_df = self.domain_dqm_check(
                                df=dqm_check_df,
                                batch_id=unprocessed_file.batch_id,
                                source_file=unprocessed_file.source_file,
                                dqm_detail=dqm_detail,
                                dataset_master=dataset_master,
                                orch_process=orch_process,
                            )

                        if dqm_detail.qc_type.lower() == "custom":
                            dqm_check_df = self.custom_dqm_check(
                                df=dqm_check_df,
                                batch_id=unprocessed_file.batch_id,
                                source_file=unprocessed_file.source_file,
                                dqm_detail=dqm_detail,
                                dataset_master=dataset_master,
                                orch_process=orch_process,
                            )
                    DeltaTableWriter(
                        input_data=dqm_check_df,
                        save_location=compute_dqm_path["s3_location"],
                        batch_id=unprocessed_file.batch_id,
                        partition_columns=dataset_master.staging_partition_columns,
                    )

                else:
                    start_time = datetime.now()
                    original_df = DeltaTableRead(
                        delta_path=data_standard_path["s3_location"],
                        batch_id=unprocessed_file.batch_id,
                    ).read()
                    DeltaTableWriter(
                        input_data=original_df,
                        save_location=compute_dqm_path["s3_location"],
                        batch_id=unprocessed_file.batch_id,
                        partition_columns=dataset_master.staging_partition_columns,
                    )
                    orch_process.insert_log_dqm(
                        log_dqm=logDqmDtl(
                            process_id=dataset_master.process_id,
                            dataset_id=dataset_master.dataset_id,
                            batch_id=unprocessed_file.batch_id,
                            source_file=unprocessed_file.source_file,
                            column_name=None,
                            qc_type=None,
                            qc_param=None,
                            qc_filter=None,
                            criticality=None,
                            criticality_threshold_pct=None,
                            error_count=0,
                            error_pct=0,
                            status="SUCCEEDED",
                            dqm_start_time=start_time,
                            dqm_end_time=datetime.now(),
                        )
                    )
