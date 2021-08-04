import pandas as pd
import pytest
from amazon_transcribe_api_client import AWSTranscribeAPIWrapper
import uuid


class TestAWSTranscribeAPIWrapper:

    def setup_class(self):
        self.client = AWSTranscribeAPIWrapper(
            # aws_access_key_id=credentials.AWS_ACCESS_KEY_ID,
            # aws_secret_access_key=credentials.AWS_SECRET_ACCESS_KEY,
            # aws_session_token=credentials.AWS_SESSION_TOKEN,
            # aws_region_name=credentials.AWS_REGION_NAME
        )
        print("SETUP !!!", self.client)

    def test_start_transcription_job(self):

        response = self.client.start_transcription_job(language="auto",
                                                       row={"path": "/test-fr.mp3"},
                                                       folder_bucket="jplassmann-transcribe-plugin",
                                                       folder_root_path="dataiku/DKU_TUTORIAL_BASICS_101/IDGRZLFi",
                                                       job_id="1111")


        assert type(response) == str

    def test__get_job_res(self):
        """ Test schema of the job result. """
        def fn(folder, job_name):
            return {
                'results': {
                    'transcripts': [
                        {"transcript": 'ceci est un deuxième test.'}
                    ]
                }
            }
        job_data = self.client._get_job_res(
            path='',
            job={"TranscriptionJobName": "", "TranscriptionJobStatus": self.client.COMPLETED, "LanguageCode": "EN"},
            display_json=False,
            function=fn,
            folder=''
        )
        assert "path" in job_data
        assert "job_name" in job_data
        assert "transcript" in job_data
        assert "language_code" in job_data
        assert "language" in job_data
        assert "output_error_type" in job_data
        assert "output_error_message" in job_data

    def test_get_results(self):

        def fn(folder, job_name):
            return {
                'results': {
                    'transcripts': [
                        {"transcript": 'ceci est un deuxième test.'}
                    ]
                }
            }

        aws_job_id = uuid.uuid4().hex
        response = self.client.start_transcription_job(language="auto",
                                                       row={"path": "/Test-fr-2.mp3"},
                                                       folder_bucket="jplassmann-transcribe-plugin",
                                                       folder_root_path="dataiku/DKU_TUTORIAL_BASICS_101/IDGRZLFi",
                                                       job_id=aws_job_id)
        submitted_jobs = pd.DataFrame.from_dict([{"path": "/Test-fr-2.mp3", "output_response": response}])
        res = self.client.get_results(submitted_jobs=submitted_jobs,
                                      recipe_job_id=aws_job_id,
                                      display_json=False,
                                      function=fn,
                                      folder="")
        transcript = res["transcript"][0]
        assert transcript == 'ceci est un deuxième test.'
