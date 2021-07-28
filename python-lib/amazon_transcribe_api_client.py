# -*- coding: utf-8 -*-
"""Module with utility functions to call the Amazon Transcribe API"""

import logging
import os
import random
import time

from typing import AnyStr, Dict, Tuple, Callable, List

import boto3
import pandas as pd
from botocore.config import Config
from botocore.exceptions import BotoCoreError
from botocore.exceptions import ClientError
from botocore.exceptions import NoRegionError

from language_dict import SUPPORTED_LANGUAGES
from plugin_io_utils import PATH_COLUMN

# ==============================================================================
# CONSTANT DEFINITION
# ==============================================================================

SLEEPING_TIME_BETWEEN_ROUNDS = 5


# ==============================================================================
# CLASS AND FUNCTION DEFINITION
# ==============================================================================


class AWSTranscribeAPIWrapper:
    SUPPORTED_AUDIO_FORMATS = ["flac", "mp3", "mp4", "ogg", "webm", "amr", "wav"]
    API_EXCEPTIONS = (ClientError, BotoCoreError)
    COMPLETED = "COMPLETED"
    QUEUED = "QUEUED"
    IN_PROGRESS = "IN_PROGRESS"
    FAILED = "FAILED"

    def __init__(self,
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
                logging.info(
                    "The region could not be loaded from environment variables. "
                    "Please specify in the plugin's API credentials settings."
                )
                logging.error(e)
                raise
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
                logging.error(e)
                raise

        logging.info("Credentials loaded.")

    def start_transcription_job(self,
                                language: AnyStr,
                                row: Dict = None,
                                folder_bucket: AnyStr = "",
                                folder_root_path: AnyStr = "",
                                job_id: AnyStr = "",
                                **kwargs,
                                ) -> AnyStr:
        """
        Function starting a transcription job given the language, the path to the audio, the job name and
        some other data about the bucket

        Returns:
            name of the job that has been submitted

        """
        audio_path = row[PATH_COLUMN]
        file_name = os.path.splitext(os.path.split(audio_path)[1])[0]
        job_name = f'{job_id}_{file_name}_{random.randint(1000, 9999)}'

        transcribe_request = {
            "TranscriptionJobName": job_name,
            "Media": {'MediaFileUri': f's3://{folder_bucket}/{folder_root_path}{audio_path}'},
            "OutputBucketName": folder_bucket,
            "OutputKey": f'{folder_root_path}/response/'
        }
        if language != "auto":
            transcribe_request["LanguageCode"] = language
        else:
            transcribe_request["IdentifyLanguage"] = True

        response = self.client.start_transcription_job(**transcribe_request)
        return response["TranscriptionJob"]["TranscriptionJobName"]

    def get_transcription_job(self,
                              job_name: AnyStr
                              ):
        """
        Get the full information of a specific job. (not used, as we prefer batch processing)

        Returns:
            Dictionary representing the job.
        """
        # deal with errors in case job_name does not exists
        response = self.client.get_transcription_job(
            TranscriptionJobName=job_name
        )
        return response["TranscriptionJob"]

    def get_list_jobs(self,
                      job_name_contains: AnyStr,
                      status: AnyStr
                      ) -> List[Dict]:
        """
        Get the list of jobs that contains "job_name_contains" in the job name and has a specific status.
        The AWS API will give a list of jobs with at most 100 jobs, to get more than that, we have to go
        through the other pages by precising the NextToken argument to the next call of the API.

        Returns:
            list of dictionary representing a summary of the jobs
        """

        response = self.client.list_transcription_jobs(
            JobNameContains=job_name_contains,
            Status=status
        )

        next_token = response.get("NextToken", None)
        result = response.get("TranscriptionJobSummaries", [])

        while next_token is not None:
            response = self.client.list_transcription_jobs(
                JobNameContains=job_name_contains,
                Status=status,
                NextToken=next_token
            )

            result += response.get("TranscriptionJobSummaries", [])
            next_token = response.get("NextToken", None)

        return result

    def get_results(self,
                    submitted_jobs,
                    recipe_job_id: AnyStr,
                    display_json: bool,
                    function: Callable,
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

        folder = kwargs["folder"]
        res = {}
        while len(submitted_jobs) != len(res):

            completed_jobs = self.get_list_jobs(recipe_job_id, self.COMPLETED)
            failed_jobs = self.get_list_jobs(recipe_job_id, self.FAILED)
            for job in completed_jobs + failed_jobs:
                job_name = job.get("TranscriptionJobName")
                job_status = job.get("TranscriptionJobStatus")
                if job_name not in res:
                    if job_status == self.COMPLETED:
                        json_results = function(folder, job_name)
                        job_data = {
                            "AWS_transcribe_job_name": job_name,
                            "transcript": json_results.get("results").get("transcripts")[0].get("transcript"),
                            "language_code": job.get("LanguageCode"),
                            "language": SUPPORTED_LANGUAGES.get(job.get("LanguageCode"))
                        }

                        if display_json:
                            job_data["json"] = json_results

                    else: # job_status == FAILED
                        job_data = {
                            "failure_reason": job.get("FailureReason")
                        }
                    res[job_name] = job_data

            time.sleep(SLEEPING_TIME_BETWEEN_ROUNDS)

        df = pd.DataFrame.from_dict(res, orient='index')
        return df