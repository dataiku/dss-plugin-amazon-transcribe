# -*- coding: utf-8 -*-
import random

from dku_io_utils import read_json_from_folder
from parallelizer import DataFrameParallelizer
from plugin_params_loader import PluginParamsLoader
from plugin_params_loader import RecipeID

# ==============================================================================
# SETUP
# ==============================================================================

recipe_job_id = str(random.randint(100000, 999999))

params = PluginParamsLoader(RecipeID.TRANSCRIBE).validate_load_params()

parallelizer = DataFrameParallelizer(function=params.api_wrapper.start_transcription_job,
                                     exceptions_to_catch=params.api_wrapper.API_EXCEPTIONS)

submitted_jobs = parallelizer.run(df=params.input_df,
                                  folder_bucket=params.input_folder_bucket,
                                  folder_root_path=params.input_folder_root_path,
                                  job_id=recipe_job_id,
                                  **vars(params))


job_results = params.api_wrapper.get_results(submitted_jobs=submitted_jobs,
                                    recipe_job_id=recipe_job_id,
                                    display_json=params.display_json,
                                    function=read_json_from_folder,
                                    folder=params.input_folder)

df = submitted_jobs.merge(job_results, right_on="AWS_transcribe_job_name", left_on="output_response")

params.output_dataset.write_with_schema(df)