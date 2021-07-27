# -*- coding: utf-8 -*-
"""Module with utility functions to call the Amazon Transcribe API"""

import logging
import os
import random
import json

from typing import AnyStr, Dict, Tuple, Callable

import boto3
from botocore.config import Config
from botocore.exceptions import BotoCoreError
from botocore.exceptions import ClientError
from botocore.exceptions import NoRegionError

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
                                **kwargs,
                                ):
        audio_path = row[PATH_COLUMN]
        file_name = os.path.splitext(os.path.split(audio_path)[1])[0]
        job_name = f'{file_name}_{random.randint(1000, 9999)}'

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

    # def start_transcription_job(self,
    #                             job_name: AnyStr,
    #                             filepath: AnyStr,
    #                             language: AnyStr,
    #                             folder_bucket: AnyStr,
    #                             folder_root_path: AnyStr,
    #                             output_bucket_name: AnyStr,
    #                             output_key: AnyStr
    #                             ):
    #     """
    #         Calls Amazon Transcribe API to start a transcription job.
    #         """
    #     # control that there is an extension to the file
    #     if not isinstance(filepath, str) or not isinstance(job_name, str):
    #         return {}, None
    #     else:
    #         filename, file_extension = os.path.splitext(filepath)
    #         file_extension = file_extension[1:]
    #         print(job_name, filepath, file_extension, language)
    #
    #         transcribe_request = {
    #             "TranscriptionJobName": job_name,
    #             "Media": {'MediaFileUri': f's3://{folder_bucket}/{folder_root_path}/{filename}.{file_extension}'},
    #             "MediaFormat": file_extension,
    #             "IdentifyLanguage": (language=="auto"),
    #             "OutputBucketName": folder_bucket,
    #             "OutputKey": f'{folder_root_path}/response/'
    #         }
    #
    #         response = self.client.start_transcription_job(
    #             TranscriptionJobName=job_name,
    #             Media={'MediaFileUri': filepath},
    #             MediaFormat=file_extension,
    #             # LanguageCode=language if language != "auto" else "Null",
    #             IdentifyLanguage=(language == "auto"),
    #             OutputBucketName=output_bucket_name,  # "jplassmann-transcribe-plugin",
    #             OutputKey=output_key[1:]  # "dataiku/DKU_TUTORIAL_BASICS_101/D5fSP1od/results/"
    #         )
    #         return response["TranscriptionJob"], f"s3://{output_bucket_name}{output_key}{job_name}.json"

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

        response = self.client.ListTranscriptionjobs(
            JobNameContains=job_name_contains,
            Status=status
        )

        return response.get("TranscriptionJobSummaries", [])
