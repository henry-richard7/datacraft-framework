from json import loads as json_loads
import jaydebeapi
from glob import glob
from dotenv import load_dotenv
from os import getenv

import polars
from datetime import datetime

from datacraft_framework.Models.schema import (
    ctlDataAcquisitionConnectionMaster,
    logDataAcquisitionDetail,
    ctlDataAcquisitionDetail,
)
from datacraft_framework.Common import OrchestrationProcess
from datacraft_framework.Common.DataProcessor import BronzeInboundWriter
from datacraft_framework.Common.FileNameGenerator import file_name_generator
from datacraft_framework.Common.S3Process import path_to_s3

import traceback
import logging

load_dotenv()

jars = getenv("jdbc_jars")
jars = glob(jars + "*.jar")
logger = logging.getLogger(__name__)
env = getenv("env")


class DatabaseExtractor:
    """
    A class to extract data from a JDBC-compatible database and write it to a structured format (e.g., CSV) or cloud storage (e.g., S3).

    This class connects to databases using `jaydebeapi`, executes queries, and writes results using `BronzeInboundWriter`.
    It also logs ingestion details into a control table via `OrchestrationProcess`.
    """

    def connect_via_jdbc(self, config: dict) -> jaydebeapi.Connection:
        """
        Connect to a JDBC-compatible database dynamically based on the config dictionary.

        Supports dynamic query parameters and URL construction for various databases like MySQL, PostgreSQL, Snowflake, etc.

        Args:
            config (dict): Connection configuration containing:
                - driver (str): JDBC driver class name.
                - url (str): Base JDBC URL (e.g., jdbc:mysql://localhost:3306).
                - user (str): Username for authentication.
                - password (str): Password for authentication.
                - Optional keys: database, schema, warehouse, jar, and other query parameters.

        Returns:
            jaydebeapi.Connection: An active connection object to the database.

        Raises:
            jaydebeapi.DatabaseError: If connection fails due to invalid credentials or unreachable server.
        """
        base_url = config["url"]
        driver = config["driver"]
        user = config["user"]
        password = config["password"]

        # Adjust URL based on database type
        jdbc_url = base_url

        # Extract database if applicable (for MySQL/Postgres-style URLs)
        db = config.get("database")

        if "mysql" in driver or "postgresql" in driver or "mariadb" in driver:
            # Append database to path, if not already present
            if db and not base_url.rstrip("/").endswith(f"/{db}"):
                jdbc_url = base_url.rstrip("/") + f"/{db}"

        # Collect query parameters for all other keys
        query_params = []
        for key, value in config.items():
            if key not in {"url", "user", "password", "driver", "jar", "database"}:
                query_params.append(f"{key}={value}")

        if query_params:
            sep = "&" if "?" in jdbc_url else "?"
            jdbc_url += sep + "&".join(query_params)

        # Connect
        conn = jaydebeapi.connect(driver, jdbc_url, [user, password], jars)
        return conn

    def __init__(
        self,
        data_acquisition_connection_master: ctlDataAcquisitionConnectionMaster,
        pre_ingestion_logs: list[logDataAcquisitionDetail],
        orch_process: OrchestrationProcess.OrchestrationProcess,
        data_acquisition_detail: ctlDataAcquisitionDetail,
    ):
        """
        Initialize and execute the database extraction process.

        Validates input configuration, connects to the database, executes query, writes result to S3,
        and logs the ingestion status.

        Args:
            data_acquisition_connection_master (ctlDataAcquisitionConnectionMaster): Connection metadata object.
            pre_ingestion_logs (list[logDataAcquisitionDetail]): List of previously processed files to avoid duplication.
            orch_process (OrchestrationProcess): Instance used for logging process events.
            data_acquisition_detail (ctlDataAcquisitionDetail): Configuration for this acquisition step.

        Raises:
            Exception: If the file has already been processed to prevent duplication.
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
        save_location_s3 = path_to_s3(
            location=data_acquisition_detail.inbound_location.rstrip("/"),
            env=env,
        )["s3_location"]
        save_location_ = f"{save_location_s3}/{file_name}"

        if save_location_ in pre_ingestion_processed_files:
            raise Exception(
                f"The file {file_name} is already processed to Bronze Layer."
            )

        start_time = datetime.now()
        batch_id = int(datetime.now().strftime("%Y%m%d%H%M%S%f")[:-1])

        try:
            connection = self.connect_via_jdbc(config=connection_config)
            cursor = connection.cursor()
            cursor.execute(data_acquisition_detail.query)
            column_names = [desc[0] for desc in cursor.description]
            data = cursor.fetchall()
            cursor.close()
            connection.close()

            df = polars.DataFrame(data=data, schema=column_names)
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
                    outbound_source_location="DATABASE",
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
                    outbound_source_location="DATABASE",
                    inbound_file_location=None,
                    status="FAILED",
                    exception_details=traceback.format_exc(),
                    start_time=start_time,
                    end_time=datetime.now(),
                )
            )
            logger.error(traceback.format_exc())
            raise
