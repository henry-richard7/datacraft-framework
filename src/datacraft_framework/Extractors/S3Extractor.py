import boto3
from botocore.client import Config
from datetime import datetime
from json import loads as json_loads

from datacraft_framework.Models.schema import (
    ctlDataAcquisitionConnectionMaster,
    logDataAcquisitionDetail,
    ctlDataAcquisitionDetail,
)
from datacraft_framework.Common import (
    PatternValidator,
    S3Process,
    OrchestrationProcess,
    DataProcessor,
)
from io import BytesIO
import logging
import traceback

logger = logging.getLogger(__name__)


class S3Extractor:
    """
    A class to extract data from Amazon S3 or S3-compatible storage services.

    This class handles listing objects in a given S3 bucket/prefix, downloading files,
    validating file names using pattern matching, and writing them to a target location.
    It also logs ingestion details into a control table via `OrchestrationProcess`.

    """

    def parse_location(self, outbound_location: str) -> dict:
        """
        Convert a raw S3 path into a structured dictionary containing bucket and prefix.

        Args:
            outbound_location (str): Raw S3 path like 'bucket-name/path/to/files/'.

        Returns:
            dict: Dictionary with keys:
                - 'Bucket': Bucket name.
                - 'Prefix': Object key prefix used for filtering.

        Examples:
            >>> self.parse_location("my-bucket/data/input/")
            {'Bucket': 'my-bucket', 'Prefix': 'data/input/'}
        """
        splited_path = outbound_location.rstrip("/").split("/")
        if splited_path[0] == "":
            splited_path.pop(0)
        bucket_name = splited_path[0]
        prefix = "/".join(splited_path[1:]) + "/"

        return {
            "Bucket": bucket_name,
            "Prefix": prefix,
        }

    def __init__(
        self,
        data_acquisition_connection_master: ctlDataAcquisitionConnectionMaster,
        pre_ingestion_logs: list[logDataAcquisitionDetail],
        orch_process: OrchestrationProcess.OrchestrationProcess,
        data_acquisition_detail: ctlDataAcquisitionDetail,
    ):
        """
        Initialize and execute the S3 extraction process.

        Validates input configuration, connects to S3, lists and downloads matching files,
        and logs ingestion status. Skips already processed files.

        Args:
            data_acquisition_connection_master (ctlDataAcquisitionConnectionMaster): Connection metadata object.
            pre_ingestion_logs (list[logDataAcquisitionDetail]): List of previously processed files to avoid duplication.
            orch_process (OrchestrationProcess): Instance for logging process events.
            data_acquisition_detail (ctlDataAcquisitionDetail): Ingestion configuration.

        Raises:
            Exception: If no new unprocessed files are found or an error occurs during download.
        """

        connection_config: dict = json_loads(
            data_acquisition_connection_master.connection_config
        )

        client_id = connection_config["client_id"]
        client_secret = connection_config["client_secret"]

        endpoint_url = connection_config.get("endpoint_url", False)
        region = connection_config.get("region", False)
        signature_version = connection_config.get("signature_version", False)

        config_ = dict()

        config_["aws_access_key_id"] = client_id
        config_["aws_secret_access_key"] = client_secret

        if endpoint_url:
            config_["endpoint_url"] = endpoint_url

        if region:
            config_["region"] = region

        if signature_version:
            config_["config"] = Config(signature_version=signature_version)

        s3_client = boto3.client(
            "s3",
            **config_,
        )

        s3_object_config = self.parse_location(
            outbound_location=data_acquisition_detail.outbound_source_location
        )

        files = s3_client.list_objects_v2(**s3_object_config).get("Contents")

        pre_ingestion_processed_files = [
            x.inbound_file_location for x in pre_ingestion_logs
        ]

        new_files = list()
        if files:
            for file in files:
                file_ = file.get("Key")
                file_name_s3 = file_.split("/")[-1]

                if PatternValidator.validate_pattern(
                    file_pattern=data_acquisition_detail.outbound_source_file_pattern,
                    file_name=file_name_s3,
                    custom=(
                        True
                        if data_acquisition_detail.outbound_source_file_pattern_static
                        == "Y"
                        else False
                    ),
                ):
                    file_save_name = f"s3a://prod-{data_acquisition_detail.inbound_location.rstrip('/')}/{file_name_s3}"

                    if file_save_name not in pre_ingestion_processed_files:
                        new_files.append(file_save_name)

                        start_time = datetime.now()
                        batch_id = int(datetime.now().strftime("%Y%m%d%H%M%S%f")[:-1])

                        try:
                            buffer = BytesIO()
                            s3_client.download_fileobj(
                                Bucket=s3_object_config["Bucket"],
                                Key=file.get("Key"),
                                Fileobj=buffer,
                            )

                            buffer.seek(0)

                            splited_ = data_acquisition_detail.inbound_location.split(
                                "/"
                            )
                            bucket_name = "prod-" + splited_[0]
                            splited_.pop(0)
                            aws_file_key = (
                                "/".join(splited_) + file_name_s3.split("/")[-1]
                            )
                            S3Process.S3Process().s3_raw_file_write(
                                file_object=buffer,
                                bucket=bucket_name,
                                file_name=aws_file_key,
                            )
                            orch_process.insert_log_data_acquisition_detail(
                                log_data_acquisition=logDataAcquisitionDetail(
                                    batch_id=batch_id,
                                    run_date=start_time.date(),
                                    process_id=data_acquisition_detail.process_id,
                                    pre_ingestion_dataset_id=data_acquisition_detail.pre_ingestion_dataset_id,
                                    outbound_source_location="S3",
                                    inbound_file_location=file_save_name,
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
                                    outbound_source_location="S3",
                                    inbound_file_location=None,
                                    status="FAILED",
                                    exception_details=traceback.format_exc(),
                                    start_time=start_time,
                                    end_time=datetime.now(),
                                )
                            )
                            logger.error(traceback.format_exc())
                            raise

            if len(new_files) == 0:
                raise Exception(f"No unprocessed files are found.")
