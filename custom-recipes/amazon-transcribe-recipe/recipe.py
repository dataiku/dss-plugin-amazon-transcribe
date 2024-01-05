# -*- coding: utf-8 -*-
import uuid
from amazon_transcribe_api_client import AWSTranscribeAPIWrapper
from dku_io_utils import read_json_from_folder, set_column_description
from dkulib.core.parallelizer import DataFrameParallelizer
from plugin_params_loader import PluginParamsLoader
from plugin_params_loader import RecipeID


# ==============================================================================
# CONSTANT/PATTERN DEFINITION
# ==============================================================================

RECIPE_JOB_ID = f"{dataiku.dku_custom_variables.get('jobId')}_{uuid.uuid4().hex}"

# ==============================================================================
# SETUP
# ==============================================================================

params = PluginParamsLoader(RecipeID.TRANSCRIBE).validate_load_params()

api_wrapper = AWSTranscribeAPIWrapper(use_timeout=params.use_timeout,
                                      timeout_min=params.timeout_min)
api_wrapper.build_client(aws_access_key_id=params.aws_access_key_id,
                         aws_secret_access_key=params.aws_secret_access_key,
                         aws_session_token=params.aws_session_token,
                         aws_region_name=params.aws_region_name,
                         max_attempts=params.max_attempts)

parallelizer = DataFrameParallelizer(function=api_wrapper.start_transcription_job,
                                     exceptions_to_catch=api_wrapper.API_EXCEPTIONS)

submitted_jobs = parallelizer.run(df=params.input_df,
                                  input_folder_bucket=params.input_folder_bucket,
                                  input_folder_root_path=params.input_folder_root_path,
                                  output_folder_bucket=params.output_folder_bucket,
                                  output_folder_root_path=params.output_folder_root_path,
                                  job_id=RECIPE_JOB_ID,
                                  language=params.language,
                                  show_speaker_labels=params.show_speaker_labels,
                                  max_speaker_labels=params.max_speaker_labels,
                                  redact_pii=params.redact_pii,
                                  pii_types=params.pii_types)

job_results = api_wrapper.get_results(submitted_jobs=submitted_jobs,
                                      recipe_job_id=RECIPE_JOB_ID,
                                      display_json=params.display_json,
                                      redact_pii=params.redact_pii,
                                      transcript_json_loader=read_json_from_folder,
                                      folder=params.output_folder)

params.output_dataset.write_with_schema(job_results)
column_description = {
    'path': 'Path to the audio file in the S3 bucket.',
    'job_name': 'Name to identify the job in Amazon Transcribe.',
    'transcript': 'Transcript of the audio file.',
    'language': 'Language detected or setup by the user.',
    'language_code': 'Language code detected or setup by the user.',
    'json': 'Raw API response in JSON form.',
    'output_error_type': 'The error type in case an error occurs.',
    'output_error_message': 'The error message in case an error occurs.'
}
set_column_description(params.output_dataset, column_description)
