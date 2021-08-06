import pandas as pd
import pytest
# import pytest_mock
from amazon_transcribe_api_client import AWSTranscribeAPIWrapper, get_cpu
import amazon_transcribe_api_client
import uuid


class TestAWSTranscribeAPIWrapper:

    def setup_class(self):
        self.client = AWSTranscribeAPIWrapper()
        print("SETUP !!!", self.client)

    def test_start_transcription_job(self, mocker):

        # Wrong job name sent should raise Exception
        with pytest.raises(Exception):
            response = self.client.start_transcription_job(language="auto",
                                                           row={"path": "/test-fr.mp3"},
                                                           folder_bucket="jplassmann-transcribe-plugin",
                                                           folder_root_path="dataiku/DKU_TUTORIAL_BASICS_101/IDGRZLFi",
                                                           job_id="1 111")
        # mocker.patch('AWSTranscribeAPIWrapper.client.start_transcription_job',
        #              return_value={"TranscriptionJob": {"TranscriptionJobName": "name"}}
        #              )
        def mock_load(self):
            return "test"
        mocker.patch.object('botocore.client.BaseClient._make_api_call', mock_load)
        actual = self.client.start_transcription_job(
            language="auto",
            row={"path": "/test-fr.mp3"},
            folder_bucket="jplassmann-transcribe-plugin",
            folder_root_path="dataiku/DKU_TUTORIAL_BASICS_101/IDGRZLFi",
            job_id="1111"
        )
        expeceted = "test"
        assert actual == expeceted

        # assert type(response) == str,

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
        job_data = self.client._result_parser(
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

    # def test_get_results(self):
    #
    #     def fn(folder, job_name):
    #         return {
    #             'results': {
    #                 'transcripts': [
    #                     {"transcript": 'ceci est un deuxième test.'}
    #                 ]
    #             }
    #         }
    #
    #     aws_job_id = uuid.uuid4().hex
    #     response = self.client.start_transcription_job(language="auto",
    #                                                    row={"path": "/Test-fr-2.mp3"},
    #                                                    folder_bucket="jplassmann-transcribe-plugin",
    #                                                    folder_root_path="dataiku/DKU_TUTORIAL_BASICS_101/IDGRZLFi",
    #                                                    job_id=aws_job_id)
    #     submitted_jobs = pd.DataFrame.from_dict([{"path": "/Test-fr-2.mp3", "output_response": response}])
    #     res = self.client.get_results(submitted_jobs=submitted_jobs,
    #                                   recipe_job_id=aws_job_id,
    #                                   display_json=False,
    #                                   function=fn,
    #                                   folder="")
    #     transcript = res["transcript"][0]
    #     assert transcript == 'ceci est un deuxième test.'
