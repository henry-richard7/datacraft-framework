from datetime import datetime
from json import loads as json_loads
import niquests
import polars
from datacraft_framework.Common.FileNameGenerator import file_name_generator

from datacraft_framework.Models.schema import (
    ctlDataAcquisitionConnectionMaster,
    ctlDataAcquisitionDetail,
    logDataAcquisitionDetail,
)
from datacraft_framework.Common import OrchestrationProcess
from datacraft_framework.Common.DataProcessor import BronzeInboundWriter

import logging
import traceback

logger = logging.getLogger(__name__)


class SalesForce:
    """
    A class for connecting to Salesforce using OAuth2 and querying data from datasets.

    This class handles authentication via OAuth2 client credentials flow and provides
    a method to query Salesforce objects with pagination support.

    Attributes:
        domain (str): Base URL of the Salesforce instance.
        headers (dict): HTTP headers including access token after successful authentication.
    """

    def __init__(
        self,
        connection_config: dict,
    ) -> None:
        """
        Initialize the Salesforce connection using provided configuration.

        Args:
            connection_config (dict): Dictionary containing:
                - 'domain': Salesforce instance base URL.
                - 'client_id': OAuth2 client ID.
                - 'client_secret': OAuth2 client secret.

        Raises:
            Exception: If authentication fails or no access token is received.
        """

        self.domain = connection_config["domain"]
        client_id = connection_config["client_id"]
        client_secret = connection_config["client_secret"]
        oauth_endpoint = "/services/oauth2/token"

        payload = {
            "grant_type": "client_credentials",
            "client_id": client_id,
            "client_secret": client_secret,
        }

        response = niquests.post(url=f"{self.domain}{oauth_endpoint}", data=payload)

        if response.status_code == 200:
            access_token = response.json()["access_token"]
            self.headers = {"Authorization": "Bearer " + access_token}
        else:
            raise Exception(f"Failed In Getting Access Token:\n {response.text}")

    def query(self, columns: list[str], dataset_name: str) -> list[dict]:
        """
        Query records from a specified Salesforce dataset (object).

        This method supports paginated responses via `nextRecordsUrl`.

        Args:
            columns (list[str]): List of column names to retrieve.
            dataset_name (str): The name of the Salesforce object (e.g., `Account`, `Contact`).

        Returns:
            list[dict]: A list of dictionaries representing the queried records.

        Examples:
            >>> sf = SalesForce(config)
            >>> records = sf.query(columns=["Id", "Name"], dataset_name="Account")
        """

        query_ = f"select {','.join(columns)} FROM {dataset_name}"

        endpoint = "/services/data/v62.0/queryAll"
        response = niquests.get(
            f"{self.domain}{endpoint}",
            headers=self.headers,
            params={"q": query_},
        ).json()

        records = response["records"]
        more_results = list()

        results = list()
        for record in records:
            results.append({column: record[column] for column in columns})

        while not response["done"]:
            response = niquests.get(
                f"{self.domain}{response['nextRecordsUrl']}",
                headers=self.headers,
            ).json()
            records_ = response["records"]

            for record in records_:
                more_results.append({column: record[column] for column in columns})

        if len(more_results) != 0:
            results = results + more_results

        return results


class SalesforceExtractor:
    """
    A class that extracts data from Salesforce and writes it to an S3-compatible storage.

    This class uses `SalesForce` to fetch data, maps the result to a Polars DataFrame,
    and writes it to cloud storage using `BronzeInboundWriter`.
    It also logs ingestion status via `OrchestrationProcess`.
    """

    def __init__(
        self,
        data_acquisition_connection_master: ctlDataAcquisitionConnectionMaster,
        pre_ingestion_logs: list[logDataAcquisitionDetail],
        orch_process: OrchestrationProcess.OrchestrationProcess,
        data_acquisition_detail: ctlDataAcquisitionDetail,
    ):
        """
        Initialize and execute the Salesforce extraction process.

        Validates input configuration, connects to Salesforce, executes query, writes result to S3,
        and logs the ingestion status.

        Args:
            data_acquisition_connection_master (ctlDataAcquisitionConnectionMaster): Connection metadata object.
            pre_ingestion_logs (list[logDataAcquisitionDetail]): List of previously processed files to avoid duplication.
            orch_process (OrchestrationProcess): Instance used for logging process events.
            data_acquisition_detail (ctlDataAcquisitionDetail): Configuration for this acquisition step.

        Raises:
            Exception: If the file has already been processed or an error occurs during execution.
        """
        connection_config: dict = json_loads(
            data_acquisition_connection_master.connection_config
        )

        pre_ingestion_processed_files = [
            x.inbound_file_location for x in pre_ingestion_logs
        ]

        file_name = file_name_generator(
            data_acquisition_detail.outbound_source_file_pattern
        )

        save_location_ = f"s3a://prod-{data_acquisition_detail.inbound_location.rstrip('/')}/{file_name}"

        if save_location_ in pre_ingestion_processed_files:
            raise Exception(
                f"The file {file_name} is already processed to Bronze Layer."
            )

        start_time = datetime.now()
        batch_id = int(datetime.now().strftime("%Y%m%d%H%M%S%f")[:-1])

        try:
            salesforce_extractor = SalesForce(
                connection_config=connection_config,
            )
            columns = data_acquisition_detail.columns.split(",")
            records = salesforce_extractor.query(
                columns=columns,
                dataset_name=data_acquisition_detail.pre_ingestion_dataset_name,
            )

            df = polars.DataFrame(records)
            BronzeInboundWriter(
                input_data=df,
                save_location=save_location_,
                outbound_file_delimiter=data_acquisition_detail.outbound_file_delimiter,
            )
            orch_process.insert_log_data_acquisition_detail(
                log_data_acquisition=logDataAcquisitionDetail(
                    batch_id=batch_id,
                    run_date=start_time.date(),
                    process_id=data_acquisition_detail.process_id,
                    pre_ingestion_dataset_id=data_acquisition_detail.pre_ingestion_dataset_id,
                    outbound_source_location="SALESFORCE/VEEVA",
                    inbound_file_location=save_location_,
                    status="SUCCEEDED",
                    start_time=start_time,
                    end_time=datetime.now(),
                )
            )
        except Exception as e:
            orch_process.insert_log_data_acquisition_detail(
                log_data_acquisition=logDataAcquisitionDetail(
                    batch_id=batch_id,
                    run_date=start_time.date(),
                    process_id=data_acquisition_detail.process_id,
                    pre_ingestion_dataset_id=data_acquisition_detail.pre_ingestion_dataset_id,
                    outbound_source_location="SALESFORCE/VEEVA",
                    inbound_file_location=None,
                    status="FAILED",
                    exception_details=traceback.format_exc(),
                    start_time=start_time,
                    end_time=datetime.now(),
                )
            )
            logger.error(traceback.format_exc())
            raise
