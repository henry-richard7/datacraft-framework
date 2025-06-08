import niquests
import base64
import jwt
from datetime import datetime, timedelta, timezone
import logging
import traceback

from json import loads as json_loads
import json
from pathlib import Path
import re

from os import getenv
from dotenv import load_dotenv

import itertools

from datacraft_framework.Common import JsonDataMapper, OrchestrationProcess
from datacraft_framework.Common.Logger import LoggerManager
from datacraft_framework.Common.S3Process import S3Process, path_to_s3
from datacraft_framework.Models.schema import (
    ctlApiConnectionsDtl,
    CtlColumnMetadata,
    logDataAcquisitionDetail,
    ctlDataAcquisitionDetail,
)
from datacraft_framework.Common.FileNameGenerator import file_name_generator
from datacraft_framework.Common.DataProcessor import BronzeInboundWriter

from typing import Union, List


load_dotenv()
env = getenv("env")


class APIAutomation:
    """
    A class used to automate API requests based on a configuration dictionary.

    This class supports authentication via multiple mechanisms and dynamic request body generation.
    It allows workflow execution over multiple API steps and handles token-based authentication.
    """

    def __init__(self, config):
        """
        Initialize the API automation instance with a configuration dictionary.

        Args:
            config (dict): A dictionary containing configurations for the API.
                Must contain required keys like 'method', 'url', etc., depending on the step type.
        """

        self.token = None
        self.config = config
        self.headers = {}
        self.params = {}
        self.data = {}
        self.json_body = {}

    def _replace_date(self, date_match) -> str:
        """
        Replace date placeholders like `$current_date-7$` with actual dates.

        Args:
            date_match (str): Placeholder string like "$current_date-7:%Y-%m$".

        Returns:
            date: Formatted date string.
        """

        if ":" in date_match:
            date_part, date_format = date_match.split(":")
            date_format = date_format.replace("$", "")
        else:
            date_part, date_format = date_match, "%Y-%m-%d"

        date_generation_match = re.search(r"-\d+", date_part)
        days_to_subtract = (
            int(date_generation_match.group()) if date_generation_match else 0
        )

        new_date = (datetime.today() + timedelta(days=days_to_subtract)).strftime(
            date_format
        )
        return new_date

    def date_parse_changer(self, body: dict) -> dict:
        """
        Replace all date placeholders in the request body with real values.

        Args:
            body (dict): The original request body possibly containing date placeholders.

        Returns:
            dict: Updated body with placeholders replaced.
        """

        body = json.dumps(body)
        date_matches = re.findall(r"\$current_date(?:-\d+)?(?::[^$]+)?\$", body)

        for date_match in date_matches:
            body = body.replace(date_match, self._replace_date(date_match))

        return json.loads(body)

    def fetch_token(self, step: dict):
        """
        Fetch an authentication token from the given endpoint using the specified auth method.

        Supported auth types:
            - oauth
            - service_account
            - basic_auth
            - custom

        Sets the result in `self.headers`.

        Args:
            step (dict): Step configuration with token-related details.

        Raises:
            ValueError: If the provided `auth_type` is not supported.
        """

        auth_type = step.get("auth_type")

        if auth_type == "oauth":
            response = niquests.request(
                method=step.get("method", "GET"),
                url=step["token_url"],
                data={
                    "grant_type": "client_credentials",
                    "client_id": step["client_id"],
                    "client_secret": step["client_secret"],
                },
            )
            response.raise_for_status()
            self.headers = {
                "Authorization": f"{step['token_type']} {response.json().get(step.get('token_path', 'access_token'))}"
            }

        elif auth_type == "service_account":
            private_key = step["private_key"]
            payload = {
                "iss": step["issuer"],
                "scope": step["scope"],
                "aud": step["token_url"],
                "exp": datetime.now(timezone.utc) + timedelta(minutes=60),
                "iat": datetime.now(timezone.utc),
            }
            jwt_token = jwt.encode(payload, private_key, algorithm="RS256")
            response = niquests.post(
                step["token_url"],
                data={
                    "grant_type": "urn:ietf:params:oauth:grant-type:jwt-bearer",
                    "assertion": jwt_token,
                },
            )
            response.raise_for_status()
            self.headers = {
                "Authorization": f"Bearer {response.json().get(step.get('token_path', 'access_token'))}"
            }

        elif auth_type == "basic_auth":
            username, password = (
                step["basic_auth"]["username"],
                step["basic_auth"]["password"],
            )
            self.headers = {
                "Authorization": "Basic "
                + base64.b64encode(f"{username}:{password}".encode()).decode()
            }

        elif auth_type == "custom":
            response = niquests.request(
                method=step["method"].upper(),
                url=step["token_url"],
            ).json()
            self.headers = {
                "Authorization": "Bearer " + response.get(step["token_path"])
            }

        else:
            raise ValueError(f"Unsupported auth_type: {auth_type}")

    def execute_request(
        self,
        method: str,
        url: str,
        headers: dict,
        params: dict,
        data: dict,
        json_body: dict,
    ) -> Union[dict, List[dict]]:
        """
        Execute an API request and return the processed response.

        Args:
            method (str): HTTP method (e.g., GET, POST).
            url (str): Full URL for the API request.
            headers (dict): Request headers.
            params (dict): Query parameters.
            data (dict): Form-encoded data.
            json_body (dict): JSON payload.

        Returns:
            JSON response parsed into a dict or list of dicts.
        """

        response = niquests.request(
            method=method,
            url=url,
            headers=headers if headers else None,
            params=params if params else None,
            data=data if data else None,
            json=json_body if json_body else None,
            verify=False,
        )
        response.raise_for_status()
        return response.json()

    def make_request(self, step) -> Union[dict, List[dict]]:
        """
        Process and execute a single API request step.

        Handles dynamic replacement of placeholder values and multiplexed requests.

        Args:
            step (dict): API step configuration including URL, method, headers, etc.

        Returns:
            Union[dict, List[dict]]: Parsed JSON response from the API.
        """

        url, method = step["url"], step.get("method", "GET").upper()
        headers = {**self.headers, **step.get("headers", {})}
        params, data, json_body = map(
            lambda k: {**getattr(self, k), **step.get(k, {})},
            ["params", "data", "json_body"],
        )

        # Replace $current_date in data and json_body
        if "$current_date" in str(data):
            data = self.date_parse_changer(data)
        if "$current_date" in str(json_body):
            json_body = self.date_parse_changer(json_body)

        if not step.get("body_values"):
            return self.execute_request(method, url, headers, params, data, json_body)

        # Process multiple body values
        responses = []
        to_perform_requests = []

        for body_value in step["body_values"]:
            keys = list(body_value.keys())
            values = list(body_value.values())

            # Create all combinations of the placeholder values
            for combination in itertools.product(*values):

                temp_json_body = json.dumps(
                    json_body
                )  # Make a copy of the original JSON string
                temp_params = json.dumps(data)

                # Replace each placeholder with the corresponding value from the combination
                for key, val in zip(keys, combination):
                    temp_json_body = temp_json_body.replace(key, val)
                    temp_params = temp_params.replace(key, val)

                to_perform_requests.append(
                    json.loads(temp_json_body)
                    if temp_json_body
                    else json.loads(temp_params)
                )

        # Making use of niquests multiplexed feature.
        with niquests.Session(multiplexed=True) as s:
            for to_perform_request in to_perform_requests:
                responses.append(
                    s.request(
                        method=method,
                        url=url,
                        headers=headers if headers else None,
                        params=params if params else None,
                        data=to_perform_request if data else None,
                        json=to_perform_request if json_body else None,
                        verify=False,
                    )
                )

        return {"values_based_response": [r.json() for r in responses]}

    def execute_workflow(self) -> Union[dict, List[dict]]:
        """
        Execute the entire configured API workflow step-by-step.

        Iterates through the `config` list and executes each step in sequence.

        Returns:
            (Union[dict, List[dict]]): Result of the final API call.
        """
        for step in self.config:
            if step["type"] == "TOKEN":
                self.fetch_token(step)
            else:
                return self.make_request(step)


class APIExtractor:
    """
    Class responsible for extracting data from APIs and optionally writing it to storage.

    Uses metadata from control tables to authenticate, map, and structure the resulting output.

    Attributes:
        result: The extracted and mapped API result.
    """

    def __init__(
        self,
        api_connection_dtls: list[ctlApiConnectionsDtl],
        column_meta_data: list[CtlColumnMetadata],
        pre_ingestion_logs: list[logDataAcquisitionDetail],
        orch_process: OrchestrationProcess.OrchestrationProcess,
        data_acquisition_detail: ctlDataAcquisitionDetail,
        write_data=True,
    ):
        """
        Initialize and execute the API extraction.

        Validates input configuration, builds the request config, fetches and maps the API response,
        and optionally writes the result to S3 or logs ingestion status.

        Args:
            api_connection_dtls (List[ctlApiConnectionsDtl]): List of API connection definitions.
            column_meta_data (List[CtlColumnMetadata]): Column schema/mapping definitions.
            pre_ingestion_logs (List[logDataAcquisitionDetail]): Logs for duplicate prevention.
            orch_process (OrchestrationProcess): Instance for logging process events.
            data_acquisition_detail (ctlDataAcquisitionDetail): Ingestion configuration.
            write_data (bool): Whether to persist output to file/store.

        Raises:
            Exception: If file has already been processed (duplicate protection).
        """

        logger = logging.getLogger(__name__)

        file_name = file_name_generator(
            data_acquisition_detail.outbound_source_file_pattern
        )
        save_location_s3 = path_to_s3(
            location=data_acquisition_detail.inbound_location.rstrip("/"),
            env=env,
        )
        save_location_ = save_location_s3["s3_location"]

        if save_location_ in [x.inbound_file_location for x in pre_ingestion_logs]:
            logger.error(f"The file {file_name} is already processed to Bronze Layer.")

            raise Exception(
                f"The file {file_name} is already processed to Bronze Layer."
            )

        start_time = datetime.now()
        batch_id = int(datetime.now().strftime("%Y%m%d%H%M%S%f")[:-1])
        json_mapping = {
            x.source_column_name: x.column_json_mapping for x in column_meta_data
        }

        config = list()

        for api_connection_dtl in api_connection_dtls:
            temp_dict = dict()
            temp_dict["method"] = api_connection_dtl.method
            temp_dict["type"] = api_connection_dtl.type

            if api_connection_dtl.type == "TOKEN":
                temp_dict["token_url"] = api_connection_dtl.token_url
                temp_dict["auth_type"] = api_connection_dtl.auth_type
                temp_dict["token_type"] = api_connection_dtl.token_type
                temp_dict["token_path"] = api_connection_dtl.token_path

                if api_connection_dtl.client_id is not None:
                    temp_dict["client_id"] = api_connection_dtl.client_id
                if api_connection_dtl.client_secret is not None:
                    temp_dict["client_secret"] = api_connection_dtl.client_secret

                if api_connection_dtl.username is not None:
                    temp_dict["username"] = api_connection_dtl.username
                if api_connection_dtl.password is not None:
                    temp_dict["password"] = api_connection_dtl.password

                if api_connection_dtl.issuer is not None:
                    temp_dict["issuer"] = api_connection_dtl.issuer
                if api_connection_dtl.scope is not None:
                    temp_dict["scope"] = api_connection_dtl.scope
                if api_connection_dtl.private_key is not None:
                    temp_dict["private_key"] = api_connection_dtl.private_key

            else:
                temp_dict["url"] = api_connection_dtl.url

                if api_connection_dtl.headers is not None:
                    temp_dict["headers"] = json_loads(api_connection_dtl.headers)
                if api_connection_dtl.params is not None:
                    temp_dict["params"] = json_loads(api_connection_dtl.params)
                if api_connection_dtl.data is not None:
                    temp_dict["data"] = json_loads(api_connection_dtl.data)
                if api_connection_dtl.json_body is not None:
                    temp_dict["json_body"] = json_loads(api_connection_dtl.json_body)
                if api_connection_dtl.body_values is not None:
                    temp_dict["body_values"] = json_loads(
                        api_connection_dtl.body_values
                    )

            config.append(temp_dict)

        try:
            api_response = APIAutomation(config=config).execute_workflow()
            if not api_response.get("values_based_response"):
                mapped_data = JsonDataMapper.JsonDataMapper(
                    mapping=json_mapping, json_data=api_response
                ).get_mapped_data()
            else:
                final_results = list()
                for response in api_response.get("values_based_response"):
                    mapped_data = JsonDataMapper.JsonDataMapper(
                        mapping=json_mapping,
                        json_data=response,
                    ).get_mapped_data()

                    final_results.extend(mapped_data)
                mapped_data = final_results

            if write_data:
                file_name = file_name_generator(file_name)

                path_to_s3_ = path_to_s3(
                    location=data_acquisition_detail.inbound_location.rstrip("/"),
                    env=env,
                )["s3_location"]
                save_location = f"{path_to_s3_}/{file_name}"

                BronzeInboundWriter(
                    input_data=mapped_data,
                    save_location=save_location,
                    outbound_file_delimiter=data_acquisition_detail.outbound_file_delimiter,
                )

                orch_process.insert_log_data_acquisition_detail(
                    log_data_acquisition=logDataAcquisitionDetail(
                        batch_id=batch_id,
                        run_date=start_time.date(),
                        process_id=data_acquisition_detail.process_id,
                        pre_ingestion_dataset_id=data_acquisition_detail.pre_ingestion_dataset_id,
                        outbound_source_location="API",
                        inbound_file_location=save_location,
                        status="SUCCEEDED",
                        start_time=start_time,
                        end_time=datetime.now(),
                    )
                )

            self.result = mapped_data
        except Exception as e:
            orch_process.insert_log_data_acquisition_detail(
                log_data_acquisition=logDataAcquisitionDetail(
                    batch_id=batch_id,
                    run_date=start_time.date(),
                    process_id=data_acquisition_detail.process_id,
                    pre_ingestion_dataset_id=data_acquisition_detail.pre_ingestion_dataset_id,
                    outbound_source_location="API",
                    inbound_file_location=None,
                    status="FAILED",
                    exception_details=traceback.format_exc(),
                    start_time=start_time,
                    end_time=datetime.now(),
                )
            )
            logger.error(
                traceback.format_exc(),
            )
            raise

    def get_json(self) -> Union[dict, List[dict]]:
        """
        Get the final extracted and mapped result.

        Returns:
            Extracted structured data (typically a list of dictionaries).
        """

        return self.result
