# -*- coding: utf-8 -*-
"""Module with utility functions to call the Amazon Transcribe API"""
import logging
import time
import uuid
from concurrent.futures import ThreadPoolExecutor, as_completed
from tqdm.auto import tqdm as tqdm_auto

from typing import AnyStr, Dict, Callable, List

import boto3
import pandas as pd
from botocore.config import Config
from botocore.exceptions import BotoCoreError
from botocore.exceptions import ClientError
from botocore.exceptions import NoRegionError

from dku_constants import SLEEPING_TIME_BETWEEN_ROUNDS_SEC
from dku_constants import SUPPORTED_LANGUAGES
from plugin_io_utils import PATH_COLUMN

# ==============================================================================
# CLASS AND FUNCTION DEFINITION
# ==============================================================================
AWS_FAILURE = "AWS_FAILURE"
NUM_CPU = 2


class APIParameterError(ValueError):
    """Custom exception raised when the AWS api parameters chosen by the user are invalid"""

    pass

class APITranscriptionJobError(ValueError):
    """Custom exception raised when the AWS API raise an exception"""

    pass


class AWSTranscribeAPIWrapper:
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
                raise APIParameterError("The region could not be loaded from environment variables. "
                                        "Please specify in the plugin's API credentials settings or "
                                        "set the environment variables.")

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
                raise APIParameterError("Error while using configured credentials.")

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
        except Exception as e:
            raise APITranscriptionJobError("Error happened when starting a transcription job"
                                           "raised by the function `start_transcription_job`."
                                           f"Full exception: {e}")

        logging.info(f"AWS transcribe job {job_name} submitted.")

        try:
            return response["TranscriptionJob"]["TranscriptionJobName"]
        except KeyError as e:
            raise KeyError('Badly formed response, expect the keys such that:'
                           'response["TranscriptionJob"]["TranscriptionJobName"] exists.'
                           f'Full exception: {e}')

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
        next_token = None
        result = []
        i = 0
        while True:
            logging.info(f"Fetching list_transcription_jobs for page {i}")

            try:
                args = {
                    "JobNameContains": job_name_contains,
                    "Status": status,
                }
                if next_token is not None:
                    args["NextToken"] = next_token
                response = self.client.list_transcription_jobs(
                    **args
                )

            except Exception as e:
                raise Exception(f"Exception raised when trying to reach the page {i} of the job list."
                                f"Full exception: {e}")

            # If next_token is not None, it means there are more than one page, so we have to loop over them
            next_token = response.get("NextToken", None)
            result += response.get("TranscriptionJobSummaries", [])
            i += 1
            if len(response.get("TranscriptionJobSummaries", [])) == 0 or next_token is None:
                break
        return result

    def get_results(self,
                    submitted_jobs: pd.DataFrame,
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

        mask = submitted_jobs["output_error_type"] == ""
        res = submitted_jobs[~mask].set_index("output_response").to_dict("index")

        submitted_jobs_dict = submitted_jobs[mask].set_index("output_response").to_dict("index")

        # res will be of the form {"job_name_0": {"job_name": str, "transcript": str, ...},
        #                             ...,
        #                          "job_name_n: {"job_name": str, "transcript": str, ...}}

        while len(submitted_jobs) != len(res):
            jobs = []
            with ThreadPoolExecutor(max_workers=NUM_CPU*2) as pool:
                futures = [
                    pool.submit(
                        fn=self.get_list_jobs,
                        job_name_contains=recipe_job_id,
                        status=status
                    ) for status in [self.COMPLETED, self.FAILED]
                ]
                for future in tqdm_auto(
                        as_completed(futures), total=2, miniters=1, mininterval=1.0
                ):
                    jobs += future.result()

            # loop over all the finished jobs whether they completed with success or failed
            for job in jobs:
                job_name = job.get("TranscriptionJobName")
                if job_name not in res:
                    job_data = self._result_parser(path=submitted_jobs_dict[job_name]["path"],
                                                 display_json=display_json,
                                                 job=job,
                                                 function=function,
                                                 **kwargs)

                    res[job_name] = job_data

            time.sleep(SLEEPING_TIME_BETWEEN_ROUNDS_SEC)

        job_results = pd.DataFrame.from_dict(res, orient='index')
        return job_results

    def _result_parser(self,
                     path: str,
                     job: dict,
                     display_json: bool,
                     function: Callable,
                     **kwargs):
        """
        Creates one row of the final DataFrame. Takes the job summary as argument and take all the needed
        data for the row, together with the reading in the json file.

        Returns:
            Dictionary {'path': str, 'job_name': str, 'transcript': str, 'language': str, 'language_code': str
                        'json': str, 'output_error_type': str, 'output_error_message': str}

        """

        try:
            job_name = job.get("TranscriptionJobName")
            job_status = job.get("TranscriptionJobStatus")
        except Exception as e:
            raise Exception('Badly formed response, missing keys in the JSON job result.'
                            f'Full exception: {e}')

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
        if job_status == self.COMPLETED:

            # Result json is being read by function. The Transcript will be there.
            json_results = function(folder, job_name)
            try:
                job_data["transcript"] = json_results.get("results").get("transcripts")[0].get("transcript")
                job_data["language_code"] = job.get("LanguageCode")
                job_data["language"] = SUPPORTED_LANGUAGES.get(job.get("LanguageCode"))
            except Exception as e:
                raise Exception('Badly formed response, missing keys in the JSON job result.'
                                f'Full exception: {e}')
            if display_json:
                job_data["json"] = json_results
            logging.info(f"AWS transcribe job {job_name} completed with success.")

        else:  # job_status == FAILED
            # if the job failed, lets report the error in the corresponding column
            try:
                job_data["output_error_type"] = AWS_FAILURE
                job_data["output_error_message"] = job.get("FailureReason")
                logging.error(
                    f"AWS transcribe job {job_name} failed. Failure reason: {job_data['output_error_message']}")
            except Exception as e:
                raise Exception('Badly formed response, missing keys in the JSON result to get failure reason.'
                                f'Full exception: {e}')

        return job_data
