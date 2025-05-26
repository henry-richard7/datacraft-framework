from os import getenv
from concurrent.futures import ThreadPoolExecutor

from datacraft_framework.GoldLayerScripts.Transformation import Transformation
from datacraft_framework.GoldLayerScripts.TransformationDataQualityCheck import (
    TransformationDataQualityCheck,
)
from datacraft_framework.Common.OrchestrationProcess import OrchestrationProcess
from datacraft_framework.Models.schema import ctlDatasetMaster

import logging
import traceback

logger = logging.getLogger(__name__)


class GoldLayer:
    """
    A class representing the Gold Layer data processing pipeline.

    This class orchestrates two key stages of a dataset in the Gold layer:
        - **Transformation**: Applies business logic like JOIN, UNION, or CUSTOM SQL to raw/transformed data.
        -  **Data Quality Management (DQM)**: Validates transformed data against defined quality rules.

    The Gold Layer typically represents clean, curated, and structured data ready for analytics or downstream use.

    Attributes:
        process_id (int): The unique identifier of the orchestration process.
    """

    def _handle_gold_layer(self, dataset_master: ctlDatasetMaster):
        """
        Process a single dataset through transformation and DQM into the Gold Layer.

        Args:
            dataset_master (ctlDatasetMaster): Dataset configuration object containing metadata,
                paths, and partitioning info.
        """
        logger.info(f"Starting Gold Layer for Dataset ID: {dataset_master.dataset_id}")
        with OrchestrationProcess() as orch_process:
            dqm_details = orch_process.get_dqm_detail(
                process_id=dataset_master.process_id,
                dataset_id=dataset_master.dataset_id,
            )
        logger.info(
            f"Starting Gold Layer Transformation for Dataset ID: {dataset_master.dataset_id}"
        )
        Transformation(
            dataset_master=dataset_master,
        )
        logger.info(
            f"Gold Layer Transformation for Dataset ID: {dataset_master.dataset_id} Completed."
        )
        logger.info(
            f"Starting Gold Layer Transformation DQM for Dataset ID: {dataset_master.dataset_id}"
        )
        TransformationDataQualityCheck(
            dqm_details=dqm_details,
            dataset_master=dataset_master,
        )
        logger.info(
            f"Gold Layer Transformation DQM for Dataset ID: {dataset_master.dataset_id} Completed."
        )

    def __init__(self, process_id: int):
        """
        Initialize and execute the Gold Layer pipeline for all datasets under the given process ID.

        Orchestrates parallel execution using thread pools based on available workers.

        Args:
            process_id (int): The ID of the current process to execute.

        Raises:
            Exception: If any transformation or DQM step fails during execution.
        """
        with OrchestrationProcess() as orch_process:
            gold_datasets = orch_process.get_dataset_master(
                process_id=process_id, dataset_type="GOLD"
            )

            with ThreadPoolExecutor(
                max_workers=min(int(getenv("max_threads")), len(gold_datasets))
            ) as executor:
                futures = [
                    executor.submit(self._handle_gold_layer, gold_dataset)
                    for gold_dataset in gold_datasets
                ]

                for future in futures:
                    try:
                        future.result()
                    except Exception as e:
                        raise
