# -*- coding: utf-8 -*-
import random

from dku_io_utils import read_json_from_folder
from parallelizer import DataFrameParallelizer
from plugin_params_loader import PluginParamsLoader
from plugin_params_loader import RecipeID

# ==============================================================================
# CONSTANT DEFINITION
# ==============================================================================

NB_DIGIT_JOB_ID = 5
RECIPE_JOB_ID = str(random.randint(10**NB_DIGIT_JOB_ID, 10**(NB_DIGIT_JOB_ID+1) - 1))

# ==============================================================================
# SETUP
# ==============================================================================

params = PluginParamsLoader(RecipeID.TRANSCRIBE).validate_load_params()

parallelizer = DataFrameParallelizer(function=params.api_wrapper.start_transcription_job,
                                     exceptions_to_catch=params.api_wrapper.API_EXCEPTIONS)

submitted_jobs = parallelizer.run(df=params.input_df,
                                  folder_bucket=params.input_folder_bucket,
                                  folder_root_path=params.input_folder_root_path,
                                  job_id=RECIPE_JOB_ID,
                                  **vars(params))


job_results = params.api_wrapper.get_results(submitted_jobs=submitted_jobs,
                                             recipe_job_id=RECIPE_JOB_ID,
                                             display_json=params.display_json,
                                             function=read_json_from_folder,
                                             folder=params.input_folder)

params.output_dataset.write_with_schema(job_results)