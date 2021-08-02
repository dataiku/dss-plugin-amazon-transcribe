import pytest
from amazon_transcribe_api_client import AWSTranscribeAPIWrapper


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


    def test_get_transcription_job(self):
        assert False


    def test_get_list_jobs(self):
        assert False

    def test_get_results(self):
        assert False
