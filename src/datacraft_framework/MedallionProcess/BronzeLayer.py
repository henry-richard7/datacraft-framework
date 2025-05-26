from os import getenv
from concurrent.futures import ThreadPoolExecutor
import logging
import traceback

from datacraft_framework.Common.Logger import LoggerManager

from datacraft_framework.Extractors import (
    ApiExtractor,
    SftpExtractor,
    S3Extractor,
    DatabaseExtractor,
    SalesforceExtractor,
)
from datacraft_framework.Models.schema import (
    ctlDataAcquisitionDetail,
    ctlDatasetMaster,
    logRawProcessDtl,
)

from datacraft_framework.Common.OrchestrationProcess import OrchestrationProcess
from datacraft_framework.Common.S3Process import path_to_s3, S3Process
from datacraft_framework.Common.PatternValidator import validate_pattern
from datacraft_framework.Common.DataProcessor import DeltaTableWriter
from datetime import datetime

from os import getenv
from dotenv import load_dotenv

load_dotenv()
env = getenv("env")
logger = logging.getLogger(__name__)


class BronzeLayer:
    """
    A class representing the Bronze Layer data ingestion process.

    This class orchestrates copying raw data from external systems (e.g., API, SFTP, S3, Database)
    into the framework’s inbound location and creates a Delta Lake table in the Bronze layer.

    Attributes:
        process_id (int): The ID of the current orchestration process.
        bronze_datasets (list[ctlDataAcquisitionDetail]): List of dataset configurations for ingestion.
        bronze_dataset_masters (list[ctlDatasetMaster]): List of metadata objects describing datasets.
    """

    def __init__(self, process_id):
        """
        Initialize the BronzeLayer with the given process ID.

        Retrieves acquisition and dataset metadata for this process.

        Args:
            process_id (int): The unique identifier of the orchestration process.
        """
        self.process_id = process_id
        LoggerManager(process_id=process_id)

        with OrchestrationProcess() as orch_process:
            self.bronze_datasets = orch_process.get_ctl_data_acquisition_detail(
                process_id=process_id
            )

            self.bronze_dataset_masters = orch_process.get_dataset_master(
                process_id=process_id,
                dataset_type="BRONZE",
            )

    def _handle_extraction(self, dataAcquisitionDetail: ctlDataAcquisitionDetail):
        """
        Handle extraction of data from an external source to the inbound storage.

        This method determines the correct extractor based on the source platform and executes it.
        Supported platforms include: `SFTP`, `S3`, `DATABASE`, `SALESFORCE`, `VEEVA`, and `API`.

        Args:
            dataAcquisitionDetail (ctlDataAcquisitionDetail): Configuration object containing connection
                and ingestion details for a specific dataset.

        Raises:
            Exception: If any error occurs during extraction; re-raised after logging.
        """

        with OrchestrationProcess() as orch_process:
            pre_ingestion_logs = orch_process.get_log_data_acquisition_detail(
                process_id=dataAcquisitionDetail.process_id,
                dataset_id=dataAcquisitionDetail.pre_ingestion_dataset_id,
                status="SUCCEEDED",
            )

            logger.info(
                f"Performaing Data Extraction for Dataset ID: {dataAcquisitionDetail.pre_ingestion_dataset_id} from Source: {dataAcquisitionDetail.outbound_source_platform}"
            )
            logger.debug(
                f"dataAcquisitionDetail: {dataAcquisitionDetail.model_dump()}",
            )

            if dataAcquisitionDetail.outbound_source_platform != "API":
                connection_dtl = orch_process.get_ctl_data_acquisition_connection_master(
                    outbound_source_platform=dataAcquisitionDetail.outbound_source_platform,
                    outbound_source_system=dataAcquisitionDetail.outbound_source_system,
                )
                if dataAcquisitionDetail.outbound_source_platform == "SFTP":

                    SftpExtractor.SftpExtractor(
                        data_acquisition_connection_master=connection_dtl,
                        pre_ingestion_logs=pre_ingestion_logs,
                        data_acquisition_detail=dataAcquisitionDetail,
                        orch_process=orch_process,
                    )

                if dataAcquisitionDetail.outbound_source_platform == "S3":

                    S3Extractor.S3Extractor(
                        data_acquisition_connection_master=connection_dtl,
                        pre_ingestion_logs=pre_ingestion_logs,
                        data_acquisition_detail=dataAcquisitionDetail,
                        orch_process=orch_process,
                    )

                if dataAcquisitionDetail.outbound_source_platform == "DATABASE":

                    DatabaseExtractor.DatabaseExtractor(
                        data_acquisition_connection_master=connection_dtl,
                        pre_ingestion_logs=pre_ingestion_logs,
                        data_acquisition_detail=dataAcquisitionDetail,
                        orch_process=orch_process,
                    )

                if (
                    dataAcquisitionDetail.outbound_source_platform == "VEEVA"
                    or dataAcquisitionDetail.outbound_source_platform == "SALESFORCE"
                ):

                    SalesforceExtractor.SalesforceExtractor(
                        data_acquisition_connection_master=connection_dtl,
                        pre_ingestion_logs=pre_ingestion_logs,
                        data_acquisition_detail=dataAcquisitionDetail,
                        orch_process=orch_process,
                    )

            elif dataAcquisitionDetail.outbound_source_platform == "API":

                ApiExtractor.APIExtractor(
                    api_connection_dtls=orch_process.get_ctl_api_connection_details(
                        dataset_id=dataAcquisitionDetail.pre_ingestion_dataset_id
                    ),
                    column_meta_data=orch_process.get_ctl_column_metadata(
                        dataset_id=dataAcquisitionDetail.pre_ingestion_dataset_id
                    ),
                    pre_ingestion_logs=pre_ingestion_logs,
                    data_acquisition_detail=dataAcquisitionDetail,
                    orch_process=orch_process,
                )

            logger.info(
                f"Performaing Data Extraction for Dataset ID: {dataAcquisitionDetail.pre_ingestion_dataset_id} from Source: {dataAcquisitionDetail.outbound_source_platform} Completed."
            )

    def _handle_raw_table_creation(self, dataset: ctlDatasetMaster):
        """
        Create a raw Delta table in the Bronze layer for files that have been ingested.

        Validates file patterns, filters out already processed files, and writes new ones to the landing zone.

        Args:
            dataset (ctlDatasetMaster): Dataset configuration including paths and partitioning info.

        Raises:
            Exception: If no new unprocessed files are found matching the pattern.
        """
        logger.info(
            f"Creating landing delta table for Dataset ID: {dataset.dataset_id}"
        )
        with OrchestrationProcess() as orch_process:
            ingestion_logs = orch_process.get_log_raw_process_dtl(
                process_id=dataset.process_id,
                dataset_id=dataset.dataset_id,
                status="SUCCEEDED",
            )

            inbound_path = path_to_s3(location=dataset.inbound_location, env=env)
            landing_path = path_to_s3(location=dataset.landing_location, env=env)
            file_pattern = dataset.inbound_file_pattern

            files_in_inbound = S3Process().s3_list_files(
                bucket=inbound_path["bucket"],
                file_name=inbound_path["key"],
            )
            files_in_inbound = [
                f"s3a://{inbound_path["bucket"]}/{x}" for x in files_in_inbound
            ]
            raw_completed_files = [x.source_file for x in ingestion_logs]

            new_files = set(files_in_inbound) - set(raw_completed_files)
            new_files = [
                x
                for x in new_files
                if validate_pattern(
                    file_pattern=file_pattern,
                    file_name=x.split("/")[-1],
                )
            ]
            if len(new_files) == 0:
                logger.info(
                    f"No new files found for Dataset ID: {dataset.dataset_id} hence failing."
                )
                raise Exception("No new files found to create raw delta table.")

            for new_file in new_files:

                batch_id = int(datetime.now().strftime("%Y%m%d%H%M%S%f")[:-1])

                start_time = datetime.now()
                try:
                    DeltaTableWriter(
                        input_data=new_file,
                        save_location=landing_path["s3_location"],
                        batch_id=batch_id,
                        partition_columns=dataset.landing_partition_columns,
                        outbound_file_delimiter=dataset.inbound_file_delimiter,
                    )
                    orch_process.insert_log_raw_process_detail(
                        log_raw_process_dtl=logRawProcessDtl(
                            batch_id=batch_id,
                            process_id=self.process_id,
                            dataset_id=dataset.dataset_id,
                            source_file=new_file,
                            landing_location=dataset.landing_location,
                            file_status="SUCCEEDED",
                            exception_details=None,
                            file_process_start_time=start_time,
                            file_process_end_time=datetime.now(),
                        )
                    )
                    logger.info(
                        f"Creating landing delta table for Dataset ID: {dataset.dataset_id} completed."
                    )

                except Exception as e:
                    orch_process.insert_log_raw_process_detail(
                        log_raw_process_dtl=logRawProcessDtl(
                            process_id=self.process_id,
                            dataset_id=dataset.dataset_id,
                            source_file=new_file,
                            landing_location=dataset.landing_location,
                            file_status="FAILED",
                            exception_details=traceback.format_exc(),
                            file_process_start_time=start_time,
                            file_process_end_time=datetime.now(),
                        )
                    )
                    logger.error(traceback.format_exc())

                    raise

    def start_extraction(self):
        """
        Start the Bronze layer ingestion process.

        Orchestrates parallel execution of:
        - Extraction from various sources (`SFTP`, `S3`, `API`, etc.)
        - Creation of Delta tables for newly arrived files

        Uses `ThreadPoolExecutor` to run multiple extractions concurrently.
        """
        # Source To Inbound Process
        with ThreadPoolExecutor(
            max_workers=min(int(getenv("max_threads")), len(self.bronze_datasets))
        ) as executor:
            futures = [
                executor.submit(self._handle_extraction, bronze_dataset)
                for bronze_dataset in self.bronze_datasets
            ]

            for future in futures:
                try:
                    future.result()
                except Exception as e:
                    raise

        # Raw Inbound To Landing Table
        with ThreadPoolExecutor(
            max_workers=min(
                int(getenv("max_threads")), len(self.bronze_dataset_masters)
            )
        ) as executor:
            futures = [
                executor.submit(self._handle_raw_table_creation, bronze_dataset_master)
                for bronze_dataset_master in self.bronze_dataset_masters
            ]

            for future in futures:
                try:
                    future.result()
                except Exception as e:
                    raise
