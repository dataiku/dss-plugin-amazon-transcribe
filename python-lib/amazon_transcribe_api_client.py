# -*- coding: utf-8 -*-
"""Module with utility functions to call the Amazon Transcribe API"""

import logging
import os
import random
import json
import time

from typing import AnyStr, Dict, Tuple, Callable

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
                 aws_access_key_id=None,
                 aws_secret_access_key=None,
                 aws_session_token=None,
                 aws_region_name=None,
                 max_attempts=20
                 ):
        """
        Gets a translation API client from AWS credentials.
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
                                ):
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
        # deal with errors in case job_name does not exists
        response = self.client.get_transcription_job(
            TranscriptionJobName=job_name
        )
        return response["TranscriptionJob"]

    def get_list_jobs(self,
                      job_name_contains: AnyStr,
                      status: AnyStr
                      ):

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

    def get_results(self, submitted_jobs, recipe_job_id, display_json, function, **kwargs):
        folder = kwargs["folder"]
        res = {}
        while len(submitted_jobs) != len(res):

            jobs = self.get_list_jobs(recipe_job_id, self.COMPLETED)
            for job in jobs:
                job_name = job.get("TranscriptionJobName")
                if job_name not in res:
                    json_results = function(folder, job_name)
                    job_data = {
                        "AWS_transcribe_job_name": job_name,
                        "transcript": json_results.get("results").get("transcripts")[0].get("transcript"),
                        "language_code": job.get("LanguageCode"),
                        "language": SUPPORTED_LANGUAGES.get(job.get("LanguageCode"))
                    }
                    if display_json:
                        job_data["json"] = json_results

                    res[job_name] = job_data

            time.sleep(5)

        df = pd.DataFrame.from_dict(res, orient='index')
        return df
