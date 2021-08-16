# -*- coding: utf-8 -*-
import uuid
from amazon_transcribe_api_client import AWSTranscribeAPIWrapper
from dku_io_utils import read_json_from_folder, set_column_description
from dkulib.core.parallelizer import DataFrameParallelizer
from plugin_params_loader import PluginParamsLoader
from plugin_params_loader import RecipeID


# ==============================================================================
# CONSTANT DEFINITION
# ==============================================================================

def get_recipe_job_id():
    return f"{dataiku.dku_custom_variables.get('jobId')}_{uuid.uuid4().hex}"


# ==============================================================================
# SETUP
# ==============================================================================

params = PluginParamsLoader(RecipeID.TRANSCRIBE).validate_load_params()

api_wrapper = AWSTranscribeAPIWrapper()
api_wrapper.build_client(aws_access_key_id=params.aws_access_key_id,
                         aws_secret_access_key=params.aws_secret_access_key,
                         aws_session_token=params.aws_session_token,
                         aws_region_name=params.aws_region_name,
                         max_attempts=params.max_attempts)

parallelizer = DataFrameParallelizer(function=api_wrapper.start_transcription_job,
                                     exceptions_to_catch=api_wrapper.API_EXCEPTIONS)

submitted_jobs = parallelizer.run(df=params.input_df,
                                  folder_bucket=params.input_folder_bucket,
                                  folder_root_path=params.input_folder_root_path,
                                  job_id=get_recipe_job_id(),
                                  language=params.language)

job_results = api_wrapper.get_results(submitted_jobs=submitted_jobs,
                                      recipe_job_id=get_recipe_job_id(),
                                      display_json=params.display_json,
                                      transcript_json_loader=read_json_from_folder,
                                      folder=params.input_folder)

params.output_dataset.write_with_schema(job_results)
set_column_description(params.output_dataset, {'path': 'Path to the audio file.'})
