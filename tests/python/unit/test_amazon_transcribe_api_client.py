from datetime import datetime
import pytest

import botocore.session
from botocore.stub import Stubber

from amazon_transcribe_api_client import AWSTranscribeAPIWrapper
import amazon_transcribe_api_client


class TestAWSTranscribeAPIWrapper:

    def setup_class(self):
        self.api_wrapper = AWSTranscribeAPIWrapper()
        self.api_wrapper.client = botocore.session.get_session().create_client("transcribe", region_name="eu-west-1")

    @pytest.fixture(autouse=True)
    def stubber(self):
        with Stubber(self.api_wrapper.client) as stubber:
            yield stubber
            stubber.assert_no_pending_responses()

    def test_start_transcription_job_exception(self, stubber):
        """
        Test that start_transcription_job function raise an APITranscriptionJobError exception
        when aws api raise an exception.
        """
        stubber.add_client_error('start_transcription_job', "BadRequestException")
        stubber.activate()

        with pytest.raises(amazon_transcribe_api_client.APITranscriptionJobError):
            response = self.api_wrapper.start_transcription_job(language="auto",
                                                                row={"path": "/test-fr.mp3"},
                                                                folder_bucket="jplassmann-transcribe-plugin",
                                                                folder_root_path="dataiku/DKU_TUTORIAL_BASICS_101/IDGRZLFi",
                                                                job_id="job_name")


    def test_start_transcription_job_succceed(self, stubber):
        """
        Test that start_transcription_job function of api wrapper returns the job name when succeeds.
        """

        stubber.add_response('start_transcription_job', {"TranscriptionJob": {"TranscriptionJobName": "job_name"}})
        stubber.activate()

        # Wrong job name sent should raise Exception
        response = self.api_wrapper.start_transcription_job(language="auto",
                                                            row={"path": "/test-fr.mp3"},
                                                            folder_bucket="jplassmann-transcribe-plugin",
                                                            folder_root_path="dataiku/DKU_TUTORIAL_BASICS_101/IDGRZLFi",
                                                            job_id="job_name")

        assert type(response) == str
        assert response == "job_name"

    def test__result_parser(self):
        """ Test schema of the job result. """
        def fn(folder, job_name):
            return {
                'results': {
                    'transcripts': [
                        {"transcript": 'ceci est un deuxième test.'}
                    ]
                }
            }
        job = {
            "TranscriptionJobName": "",
            "TranscriptionJobStatus": self.api_wrapper.COMPLETED,
            "CreationTime": datetime(year=2021, month=8, day=5, tzinfo=None),
            "LanguageCode": "EN"
        }
        job_data = self.api_wrapper._result_parser(
            path='',
            job=job,
            display_json=False,
            transcript_json_loader=fn,
            folder=''
        )
        assert "path" in job_data
        assert "job_name" in job_data
        assert "transcript" in job_data
        assert "language_code" in job_data
        assert "language" in job_data
        assert "output_error_type" in job_data
        assert "output_error_message" in job_data
