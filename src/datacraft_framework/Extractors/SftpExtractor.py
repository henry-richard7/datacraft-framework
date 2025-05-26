import paramiko
from io import StringIO, BytesIO
from json import loads as json_loads
from datetime import datetime
import logging
import traceback

from datacraft_framework.Models.schema import (
    ctlDataAcquisitionConnectionMaster,
    logDataAcquisitionDetail,
    ctlDataAcquisitionDetail,
)
from datacraft_framework.Common import PatternValidator, S3Process, OrchestrationProcess

logger = logging.getLogger(__name__)


class SftpExtractor:
    """
    A class to extract data from an SFTP server and store it in cloud storage (e.g., S3).

    This class connects to a remote SFTP server, lists files in a specified directory,
    downloads matching files (based on file pattern), and uploads them to a target location (e.g., S3).
    It also logs each file transfer in a control table via `OrchestrationProcess`.

    Attributes:
        `None`: All operations are stateless and executed during initialization.
    """

    def __init__(
        self,
        data_acquisition_connection_master: ctlDataAcquisitionConnectionMaster,
        pre_ingestion_logs: list[logDataAcquisitionDetail],
        orch_process: OrchestrationProcess.OrchestrationProcess,
        data_acquisition_detail: ctlDataAcquisitionDetail,
    ):
        """
        Initialize and execute the SFTP extraction process.

        Validates input configuration, connects to the SFTP server, downloads matching files,
        uploads them to cloud storage, and logs ingestion status. Skips already processed files.

        Args:
            data_acquisition_connection_master (ctlDataAcquisitionConnectionMaster): Connection metadata object.
            pre_ingestion_logs (list[logDataAcquisitionDetail]): List of previously processed files to avoid duplication.
            orch_process (OrchestrationProcess): Instance used for logging process events.
            data_acquisition_detail (ctlDataAcquisitionDetail): Configuration for this acquisition step.

        Raises:
            Exception: If no unprocessed files are found or an error occurs during transfer.
        """
        connection_config: dict = json_loads(
            data_acquisition_connection_master.connection_config
        )

        ssh = paramiko.SSHClient()
        ssh.set_missing_host_key_policy(paramiko.AutoAddPolicy())

        if data_acquisition_connection_master.ssh_private_key:
            privatekeyfile = StringIO(
                data_acquisition_connection_master.ssh_private_key
            )
            mykey = paramiko.Ed25519Key.from_private_key(privatekeyfile)
        else:
            mykey = None

        ssh.connect(
            hostname=connection_config.get("host"),
            password=connection_config.get("password"),
            username=connection_config.get("user"),
            allow_agent=True,
            pkey=mykey,
        )

        sftp = ssh.open_sftp()
        files = sftp.listdir(data_acquisition_detail.outbound_source_location)

        pre_ingestion_processed_files = [
            x.inbound_file_location for x in pre_ingestion_logs
        ]

        new_files = list()

        for file_ in files:

            save_location_ = f"s3a://prod-{data_acquisition_detail.inbound_location.rstrip('/')}/{file_}"

            if save_location_ not in pre_ingestion_processed_files:

                try:
                    if PatternValidator.validate_pattern(
                        file_pattern=data_acquisition_detail.outbound_source_file_pattern,
                        file_name=file_,
                        custom=(
                            True
                            if data_acquisition_detail.outbound_source_file_pattern_static
                            == "Y"
                            else False
                        ),
                    ):
                        new_files.append(save_location_)

                        start_time = datetime.now()
                        batch_id = int(datetime.now().strftime("%Y%m%d%H%M%S%f")[:-1])

                        remote_file = sftp.file(
                            data_acquisition_detail.outbound_source_location + file_,
                            mode="r",
                        )

                        buffer = BytesIO()

                        while True:
                            data = remote_file.read(524288000)
                            if not data:
                                break
                            buffer.write(data)
                        buffer.seek(0)

                        splited_ = data_acquisition_detail.inbound_location.split("/")
                        bucket_name = "prod-" + splited_[0]
                        splited_.pop(0)
                        aws_file_key = "/".join(splited_) + file_.split("/")[-1]

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
                                outbound_source_location=data_acquisition_detail.outbound_source_location,
                                inbound_file_location=f"s3a://prod-{data_acquisition_detail.inbound_location.rstrip('/')}/{file_}",
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
                            outbound_source_location=data_acquisition_detail.outbound_source_location,
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
            logger.error(f"No unprocessed files are found.")
            raise Exception(f"No unprocessed files are found.")
