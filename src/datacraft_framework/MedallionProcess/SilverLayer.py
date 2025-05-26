from os import getenv
from concurrent.futures import ThreadPoolExecutor
import logging
import traceback

from datacraft_framework.Models.schema import ctlDatasetMaster
from datacraft_framework.Common.OrchestrationProcess import OrchestrationProcess
from datacraft_framework.SilverLayerScripts.DataStandardization import (
    DataStandardization,
)
from datacraft_framework.SilverLayerScripts.DataQualityCheck import DataQualityCheck

logger = logging.getLogger(__name__)


class SilverLayer:
    """
    A class representing the Silver Layer data processing pipeline.

    The Silver Layer is responsible for:

    - **Data Standardization**: Cleaning, transforming, and normalizing raw data.
      Supported operations include padding, trimming, regex replacement, type conversion, etc.
    - **Data Quality Management (DQM)**: Validating standardized data against defined rules.

    This class orchestrates parallel execution of standardization and DQM processes
    across multiple datasets using thread pools.

    Attributes:
        process_id (int): The unique identifier of the orchestration process.
        datasets (List[ctlDatasetMaster]): List of dataset metadata objects for Silver layer processing.
    """

    def _handle_silver_process(self, dataset_master: ctlDatasetMaster):
        """
        Handle the Silver Layer process for a single dataset.

        Orchestrates:

            - Retrieval of data standardization and DQM rules from control tables.
            - Execution of `DataStandardization` and `DataQualityCheck`.

        Args:
            dataset_master (ctlDatasetMaster): Dataset configuration object containing paths,
                process ID, and transformation logic.

        Raises:
            Exception: If any error occurs during standardization or DQM steps.
        """
        with OrchestrationProcess() as orch_process:
            data_standard = orch_process.get_data_standard_dtl(
                dataset_id=dataset_master.dataset_id
            )
            dqm_details = orch_process.get_dqm_detail(
                process_id=dataset_master.process_id,
                dataset_id=dataset_master.dataset_id,
            )

        logger.info(
            f"Started Silver Layer for Dataset ID: {dataset_master.dataset_id}."
        )
        logger.info(
            f"\tPerforming Data DataStandardization for Dataset ID: {dataset_master.dataset_id}."
        )
        DataStandardization(
            data_standard_detail=data_standard,
            dataset_master=dataset_master,
        )
        logger.info(
            f"Completed Data DataStandardization for Dataset ID: {dataset_master.dataset_id}."
        )

        logger.info(
            f"\tPerforming Data Quality Checks for Dataset ID: {dataset_master.dataset_id}."
        )
        DataQualityCheck(dqm_details=dqm_details, dataset_master=dataset_master)
        logger.info(
            f"Completed Data DataStandardization for Dataset ID: {dataset_master.dataset_id}."
        )

    def __init__(self, process_id):
        """
        Initialize and execute the Silver Layer pipeline for all datasets under the given process ID.

        Retrieves Bronze layer datasets and spawns threads to handle standardization and DQM in parallel.

        Args:
            process_id (int): The ID of the current orchestration process.

        Raises:
            Exception: If no datasets are found for the given process ID or if any thread raises an exception.
        """
        self.process_id = process_id

        with OrchestrationProcess() as orch_process:

            self.datasets = orch_process.get_dataset_master(
                process_id=process_id,
                dataset_type="BRONZE",
            )

        with ThreadPoolExecutor(
            max_workers=min(int(getenv("max_threads")), len(self.datasets))
        ) as executor:
            futures = [
                executor.submit(self._handle_silver_process, silver_dataset)
                for silver_dataset in self.datasets
            ]

            for future in futures:
                try:
                    future.result()
                except Exception as e:
                    raise
