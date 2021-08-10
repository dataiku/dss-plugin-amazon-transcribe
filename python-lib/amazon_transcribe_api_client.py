# -*- coding: utf-8 -*-
"""Module with utility functions to call the Amazon Transcribe API"""
import logging
import time
import datetime
import uuid

from typing import AnyStr, Dict, Callable, List

import boto3
import pandas as pd
from botocore.config import Config
from botocore.exceptions import BotoCoreError
from botocore.exceptions import ClientError
from botocore.exceptions import ParamValidationError
from botocore.exceptions import NoRegionError

from dku_constants import SLEEPING_TIME_BETWEEN_ROUNDS_SEC
from dku_constants import SUPPORTED_LANGUAGES
from plugin_io_utils import PATH_COLUMN

# ==============================================================================
# CLASS AND FUNCTION DEFINITION
# ==============================================================================
AWS_FAILURE = "AWS_FAILURE"
JOB_TIMEOUT_ERROR_TYPE = "JOB_TIMEOUT_ERROR"
JOB_TIMEOUT_ERROR_MESSAGE = "The job duration lasted more than the timeout."
NUM_CPU = 2
TIMEOUT_MIN = 60


class APIParameterError(ValueError):
    """Custom exception raised when the AWS api parameters chosen by the user are invalid."""

    pass


class APITranscriptionJobError(Exception):
    """Custom exception raised when the AWS API raise an exception."""

    pass


class ResponseFormatError(ValueError):
    """Custom exception raised when the response format is wrong."""

    pass


class UnknownStatusError(ValueError):
    """Custom exception raised when the AWS API returns a job status unknown."""
    pass


class AWSTranscribeAPIWrapper:
    API_EXCEPTIONS = (ClientError, BotoCoreError)
    COMPLETED = "COMPLETED"
    QUEUED = "QUEUED"
    IN_PROGRESS = "IN_PROGRESS"
    FAILED = "FAILED"

    def __init__(self):
        self.client = None

    def build_client(self,
                     aws_access_key_id: AnyStr = None,
                     aws_secret_access_key: AnyStr = None,
                     aws_session_token: AnyStr = None,
                     aws_region_name: AnyStr = None,
                     max_attempts: int = 20
                     ):
        """
        Initialize the client by creating an AWS client with the specified credentials.
        """
        # Try to ascertain credentials from environment
        if aws_access_key_id is None or aws_access_key_id == "":
            logging.info("Attempting to load credentials from environment.")
            try:
                self.client = boto3.client(
                    service_name="transcribe", config=Config(retries={"max_attempts": max_attempts})
                )
            except NoRegionError as e:
                message = "The region could not be loaded from environment variables. " + \
                          "Please specify in the plugin's API credentials settings or " + \
                          f"set the environment variables. Full error: {e}"
                logging.error(message)
                raise APIParameterError(message)

        # Use configured credentials
        else:
            try:
                self.client = boto3.client(
                    service_name="transcribe",
                    aws_access_key_id=aws_access_key_id,
                    aws_secret_access_key=aws_secret_access_key,
                    aws_session_token=aws_session_token,
                    region_name=aws_region_name,
                    config=Config(retries={"max_attempts": max_attempts}),
                )
            except ClientError as e:
                message = f"Error while using configured credentials. Full exception: {e}"
                logging.error(message)
                raise APIParameterError(message)

        logging.info("Credentials loaded.")

    def start_transcription_job(self,
                                language: AnyStr,
                                row: Dict = None,
                                folder_bucket: AnyStr = "",
                                folder_root_path: AnyStr = "",
                                job_id: AnyStr = ""
                                ) -> AnyStr:
        """
        Function starting a transcription job given the language, the path to the audio, the job name and
        the path connected to the dataiku Folder in the bucket.

        Returns:
            name of the job that has been submitted

        """
        audio_path = row[PATH_COLUMN]

        # Generate a unique job_name for AWS Transcribe
        aws_job_id = uuid.uuid4().hex
        job_name = f'{job_id}_{aws_job_id}'

        transcribe_request = {
            "TranscriptionJobName": job_name,
            "Media": {'MediaFileUri': f's3://{folder_bucket}/{folder_root_path}{audio_path}'},
            "OutputBucketName": folder_bucket,
            "OutputKey": f'{folder_root_path}/response/'
        }
        if language == "auto":
            transcribe_request["IdentifyLanguage"] = True
        else:
            transcribe_request["LanguageCode"] = language

        try:
            response = self.client.start_transcription_job(**transcribe_request)
        except ClientError as e:
            message = "Error happened when starting a transcription job" + \
                      "raised by the function `start_transcription_job`." + \
                      f"Full exception: {e}"
            logging.error(message)
            raise APITranscriptionJobError(message)
        except ParamValidationError as e:
            message = f"The parameters you provided are incorrect. Full exception: {e}"
            logging.error(message)
            raise APIParameterError(message)

        logging.info(f"AWS transcribe job {job_name} submitted.")

        try:
            return response["TranscriptionJob"]["TranscriptionJobName"]
        except KeyError as e:
            message = 'Badly formed response, expect the keys such that:' + \
                      'response["TranscriptionJob"]["TranscriptionJobName"] exists.' + \
                      f'Full exception: {e}'
            logging.error(message)
            raise KeyError(message)

    def get_list_jobs(self,
                      job_name_contains: AnyStr,
                      ) -> List[Dict]:
        """
        Get the list of jobs that contains "job_name_contains" in the job name.
        The AWS API will give a list of jobs with at most 100 jobs, to get more than that, we have to go
        through the other pages by precising the NextToken argument to the next call of the API.

        Returns:
            list of dictionary representing a summary of the jobs
        """
        next_token = None
        result = []
        i = 0
        logging.info(f"Fetching list_transcription_jobs:")
        while True:

            try:
                args = {
                    "JobNameContains": job_name_contains,
                }
                if next_token is not None:
                    args["NextToken"] = next_token
                response = self.client.list_transcription_jobs(
                    **args
                )

            except Exception as e:
                message = f"Exception raised when trying to reach the page {i} of the job list. Full exception: {e}"
                logging.error(message)
                raise APITranscriptionJobError(message)

            # If next_token is not None, it means there are more than one page, so we have to loop over them
            next_token = response.get("NextToken", None)
            result += response.get("TranscriptionJobSummaries", [])
            i += 1
            if len(response.get("TranscriptionJobSummaries", [])) == 0 or next_token is None:
                break

        for job in result:
            date_job_created = job.get("CreationTime")
            date_job_completion = job.get("CompletionTime")
            if date_job_completion is None:
                date_job_completion = datetime.datetime.now(tz=date_job_created.tzinfo)
            time_delta_sec = (date_job_completion - date_job_created).seconds
            logging.info(f"{job.get('TranscriptionJobStatus')} | {job.get('TranscriptionJobName')} | {time_delta_sec} sec")

        return result

    def get_results(self,
                    submitted_jobs: pd.DataFrame,
                    recipe_job_id: AnyStr,
                    display_json: bool,
                    transcript_json_loader: Callable,
                    **kwargs):

        """
        Create a Pandas DataFrame with the results of the different submitted jobs.
        This function is supposed to read json files contained in an S3 bucket.
        The function argument is the function to read the json in a Dataiku Folder and
        the Folder object will be given in kwargs argument. This form is easier to test
        and to create a module that has no dependence with dataiku.

        Returns:
            DataFrame containing the transcript, the language, the job name, full json if requested
            and an error if the job failed.

        """

        mask = submitted_jobs["output_error_type"] == ""
        res = submitted_jobs[~mask].set_index("output_response").to_dict("index")

        submitted_jobs_dict = submitted_jobs[mask].set_index("output_response").to_dict("index")

        # res will be of the form {"job_name_0": {"job_name": str, "transcript": str, ...},
        #                             ...,
        #                          "job_name_n: {"job_name": str, "transcript": str, ...}}

        while True:
            jobs = self.get_list_jobs(job_name_contains=recipe_job_id)

            # loop over all jobs
            for job in jobs:
                job_name = job.get("TranscriptionJobName")
                if job_name not in res:
                    job_data = self._result_parser(path=submitted_jobs_dict[job_name]["path"],
                                                   display_json=display_json,
                                                   job=job,
                                                   transcript_json_loader=transcript_json_loader,
                                                   **kwargs)
                    if job_data is not None:
                        res[job_name] = job_data

            pending_jobs = [
                job for job in jobs if job.get("TranscriptionJobName") not in res and \
                                       job.get("TranscriptionJobStatus") in [AWSTranscribeAPIWrapper.QUEUED,
                                                                             AWSTranscribeAPIWrapper.IN_PROGRESS]
            ]
            if len(pending_jobs) == 0:
                break

            time.sleep(SLEEPING_TIME_BETWEEN_ROUNDS_SEC)

        job_results = pd.DataFrame.from_dict(res, orient='index')
        return job_results

    def _result_parser(self,
                       path: str,
                       job: dict,
                       display_json: bool,
                       transcript_json_loader: Callable,
                       **kwargs):
        """
        Creates one row of the final DataFrame. Takes the job summary as argument and take all the needed
        data for the row, together with the reading in the json file.

        Returns:
            Dictionary {'path': str, 'job_name': str, 'transcript': str, 'language': str, 'language_code': str
                        'json': str, 'output_error_type': str, 'output_error_message': str}

        """

        job_name = job.get("TranscriptionJobName")
        job_status = job.get("TranscriptionJobStatus")

        # dataiku folder or custom folder for testing
        folder = kwargs["folder"]

        job_data = {
            "path": path,
            "job_name": job_name,
            "transcript": "",
            "language_code": "",
            "language": "",
            "output_error_type": "",
            "output_error_message": ""
        }
        date_job_created = job.get("CreationTime")
        now = datetime.datetime.now(tz=date_job_created.tzinfo)
        time_delta_min = (now - date_job_created).seconds / 60
        if job_status in [AWSTranscribeAPIWrapper.QUEUED, AWSTranscribeAPIWrapper.IN_PROGRESS]:
            if time_delta_min > TIMEOUT_MIN:
                job_data["output_error_type"] = JOB_TIMEOUT_ERROR_TYPE
                job_data["output_error_message"] = JOB_TIMEOUT_ERROR_MESSAGE
            else:
                return None
        elif job_status == AWSTranscribeAPIWrapper.COMPLETED:

            # Result json is being read by function. The Transcript will be there.
            json_results = transcript_json_loader(folder, job_name)
            try:
                job_data["transcript"] = json_results.get("results").get("transcripts")[0].get("transcript")
                job_data["language_code"] = job.get("LanguageCode")
                job_data["language"] = SUPPORTED_LANGUAGES.get(job.get("LanguageCode"))
            except Exception as e:
                message = 'Badly formed response, missing keys in the JSON job result.' + \
                          f'Full exception: {e}'
                logging.error(message)
                raise ResponseFormatError(message)
            if display_json:
                job_data["json"] = json_results
            logging.info(f"AWS transcribe job {job_name} completed with success.")

        elif job_status == AWSTranscribeAPIWrapper.FAILED:
            # if the job failed, lets report the error in the corresponding column

            job_data["output_error_type"] = AWS_FAILURE
            job_data["output_error_message"] = job.get("FailureReason")
            logging.error(
                f"AWS transcribe job {job_name} failed. Failure reason: {job_data['output_error_message']}")

        else:
            logging.warning(f"Unknown state encountered: {job_status}")
            raise UnknownStatusError(f"Unknown state encountered: {job_status}")
        return job_data
