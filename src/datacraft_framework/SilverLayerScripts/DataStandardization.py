from datetime import datetime
import polars
from json import loads as json_loads
from os import getenv
from dotenv import load_dotenv
import traceback

from datacraft_framework.Common.SchemaCaster import SchemaCaster
from datacraft_framework.Common.DataProcessor import DeltaTableRead, DeltaTableWriter
from datacraft_framework.Common.S3Process import path_to_s3
from datacraft_framework.Common.OrchestrationProcess import OrchestrationProcess
from datacraft_framework.Models.schema import (
    ctlDatasetMaster,
    ctlDataStandardisationDtl,
    logDataStandardisationDtl,
)


load_dotenv()
env = getenv("env")


class DataStandardization:
    """
    Handles the data standardization process for unprocessed files in the SILVER layer.

    This class reads data from the landing location, applies transformations based on
    data standardization rules, and writes the standardized data to a specified S3 location.
    It also logs the status and any exceptions encountered during the process.

    Attributes:
        data_standard_detail (list[ctlDataStandardisationDtl]): List of standardization rules to apply.
        dataset_master (ctlDatasetMaster): Metadata for the dataset being processed.
    """

    def __init__(
        self,
        data_standard_detail: list[ctlDataStandardisationDtl],
        dataset_master: ctlDatasetMaster,
    ):
        """
        Initializes the DataStandardization process and executes it.

        Args:
            data_standard_detail (list[ctlDataStandardisationDtl]):
                A list of standardization rules such as padding, trimming, and type conversion.
            dataset_master (ctlDatasetMaster):
                Master metadata of the dataset containing details like process ID, dataset ID,
                and storage locations.

        Raises:
            Exception: If no unprocessed files are found for the given dataset.
            Exception: If an unsupported function or padding type is encountered.
            Exception: For any failure during the standardization process, details are logged.
        """

        with OrchestrationProcess() as orch_process:

            unprocessed_files = orch_process.get_data_standardisation_unprocessed_files(
                process_id=dataset_master.process_id,
                dataset_id=dataset_master.dataset_id,
            )
            data_standard_location = path_to_s3(
                location=dataset_master.data_standardisation_location, env=env
            )
            landing_location_ = path_to_s3(
                location=dataset_master.landing_location,
                env=env,
            )

            if len(unprocessed_files) != 0:

                for unprocessed_file in unprocessed_files:
                    start_time = datetime.now()
                    column_meta = orch_process.get_ctl_column_metadata(
                        dataset_id=dataset_master.dataset_id,
                    )
                    rename_mapping = {
                        x.source_column_name: x.column_name for x in column_meta
                    }

                    df = DeltaTableRead(
                        delta_path=landing_location_["s3_location"],
                        batch_id=unprocessed_file.batch_id,
                    ).read()
                    df = df.rename(rename_mapping)
                    df = SchemaCaster(df=df, column_metadata=column_meta).start()

                    if len(data_standard_detail) != 0:
                        for data_standard in data_standard_detail:
                            try:
                                if data_standard.function_name == "padding":
                                    parsed_json = json_loads(
                                        data_standard.function_params
                                    )
                                    padding_type = parsed_json["type"]
                                    padding_length = int(parsed_json["length"])
                                    padding_value = parsed_json["padding_value"]

                                    if padding_type == "left":
                                        df = df.with_columns(
                                            polars.col(
                                                data_standard.column_name
                                            ).str.pad_start(
                                                fill_char=padding_value,
                                                length=padding_length,
                                            )
                                        )
                                    elif padding_type == "right":
                                        df = df.with_columns(
                                            polars.col(
                                                data_standard.column_name
                                            ).str.pad_end(
                                                fill_char=padding_value,
                                                length=padding_length,
                                            )
                                        )
                                    else:
                                        raise Exception(
                                            f"Unsuppored Padding type: {padding_value}"
                                        )

                                elif data_standard.function_name == "trim":
                                    df = df.with_columns(
                                        polars.col(
                                            data_standard.column_name
                                        ).str.strip_chars()
                                    )

                                elif data_standard.function_name == "blank_conversion":
                                    df = df.with_columns(
                                        polars.col(data_standard.column_name)
                                        .str.strip_chars()
                                        .replace(pattern=r"\s+", value=" ")
                                    )

                                elif data_standard.function_name == "replace":
                                    parsed_json = json_loads(
                                        data_standard.function_params
                                    )
                                    to_replace_pattern = parsed_json["value"]
                                    replacement = parsed_json["value"]

                                    df = df.with_columns(
                                        polars.col(
                                            data_standard.column_name
                                        ).str.replace(
                                            pattern=to_replace_pattern,
                                            value=replacement,
                                        )
                                    )

                                elif data_standard.function_name == "type_conversion":
                                    parsed_json = json_loads(
                                        data_standard.function_params
                                    )
                                    type_conversion_type = parsed_json["type"]

                                    if type_conversion_type == "lower":
                                        df = df.with_columns(
                                            polars.col(
                                                data_standard.column_name
                                            ).str.to_lowercase()
                                        )
                                    elif type_conversion_type == "upper":
                                        df = df.with_columns(
                                            polars.col(
                                                data_standard.column_name
                                            ).str.to_lowercase()
                                        )

                                elif data_standard.function_name == "sub_string":
                                    parsed_json = json_loads(
                                        data_standard.function_params
                                    )

                                    start_index = int(parsed_json["start_index"])
                                    length = int(parsed_json["length"])

                                    df = df.with_columns(
                                        polars.col(data_standard.column_name).str.slice(
                                            offset=start_index,
                                            length=length,
                                        )
                                    )
                                else:
                                    raise Exception(
                                        f"Unknown Data Standardization Function: {data_standard.function_name}"
                                    )
                            except Exception as e:
                                orch_process.insert_data_standardisation_log(
                                    log_data_standardisation=logDataStandardisationDtl(
                                        batch_id=unprocessed_file.batch_id,
                                        process_id=dataset_master.process_id,
                                        dataset_id=dataset_master.dataset_id,
                                        source_file=unprocessed_file.source_file,
                                        data_standardisation_location=dataset_master.data_standardisation_location,
                                        status="FAILED",
                                        exception_details=traceback.format_exc(),
                                        start_datetime=start_time,
                                        end_datetime=datetime.now(),
                                    )
                                )

                        DeltaTableWriter(
                            input_data=df,
                            save_location=data_standard_location["s3_location"],
                            batch_id=unprocessed_file.batch_id,
                            partition_columns=dataset_master.data_standardisation_partition_columns,
                        )
                        orch_process.insert_data_standardisation_log(
                            log_data_standardisation=logDataStandardisationDtl(
                                batch_id=unprocessed_file.batch_id,
                                process_id=dataset_master.process_id,
                                dataset_id=dataset_master.dataset_id,
                                source_file=unprocessed_file.source_file,
                                data_standardisation_location=dataset_master.data_standardisation_location,
                                status="SUCCEEDED",
                                start_datetime=start_time,
                                end_datetime=datetime.now(),
                            )
                        )
                    else:

                        DeltaTableWriter(
                            input_data=df,
                            save_location=data_standard_location["s3_location"],
                            batch_id=unprocessed_file.batch_id,
                            partition_columns=dataset_master.data_standardisation_partition_columns,
                        )
                        orch_process.insert_data_standardisation_log(
                            log_data_standardisation=logDataStandardisationDtl(
                                batch_id=unprocessed_file.batch_id,
                                process_id=dataset_master.process_id,
                                dataset_id=dataset_master.dataset_id,
                                source_file=unprocessed_file.source_file,
                                data_standardisation_location=dataset_master.data_standardisation_location,
                                status="SUCCEEDED",
                                start_datetime=start_time,
                                end_datetime=datetime.now(),
                            )
                        )

            else:
                raise Exception(
                    f"No Unprocessed files found for SILVER Layer for DatasetID: {dataset_master.dataset_id}"
                )
