# -*- coding: utf-8 -*-
import os
import json
import ast
import random
import time
from concurrent.futures import ThreadPoolExecutor, as_completed


import dataiku
import pandas as pd
from dataiku.customrecipe import get_recipe_config
from dataiku.customrecipe import get_input_names_for_role
from dataiku.customrecipe import get_output_names_for_role
from typing import AnyStr

from dku_io_utils import read_json_from_folder
from parallelizer import DataFrameParallelizer
from plugin_params_loader import PluginParamsLoader
from plugin_params_loader import RecipeID



# ==============================================================================
# SETUP
# ==============================================================================
recipe_job_id = random.randint(100000, 999999)
params = PluginParamsLoader(RecipeID.TRANSCRIBE).validate_load_params()

parallelizer = DataFrameParallelizer(params.api_wrapper.start_transcription_job)
submitted_jobs = parallelizer.run(df=params.input_df,
                                  folder_bucket=params.input_folder_bucket,
                                  folder_root_path=params.input_folder_root_path,
                                  **vars(params))

# submitted_jobs = parallelizer(
#     function=params.api_wrapper.start_transcription_job,
#     exceptions=params.api_wrapper.API_EXCEPTIONS,
#     folder_bucket=params.input_folder_bucket,
#     folder_root_path=params.input_folder_root_path,
#     **vars(params)
# )


res = {}
while len(submitted_jobs) != len(res):

    jobs = params.api_wrapper.get_transcription_job(recipe_job_id, params.api_wrapper.COMPLETED)
    for job in jobs:
        job_name = job.get("TranscriptionJobName")
        if job_name not in res:
            json_results = read_json_from_folder(params.input_folder, job_name)
            job_data = {
                "path": f'',
                "transcript": json_results.get("results").get("transcripts").get(0).get("transcript"),
                "lang": job.get("LanguageCode"),
            }
            if params.displayJSON:
                job_data["JSON"] = json_results
            res[job_name] = job_data

    time.sleep(5)

df = pd.DataFrame(res)

params.output_dataset.write_with_schema(df)

# def custom_function(job_name: AnyStr):
#
#     result = params.api_wrapper.get_transcription_job(job_name)
#
#     status = result.get("TranscriptionJobStatus")
#     if status == "COMPLETED":
#         json_results = read_json_from_folder(params.input_folder, job_name)
#     else:
#         json_results = {}
#
#     return json_results, status, job_name
#
# submitted_jobs = submitted_jobs.set_index("transcribe_response")
# submitted_jobs_dict = submitted_jobs.to_dict('index')


# results = api_parallelizer_getting_results(
#     function=custom_function,
#     jobs=submitted_jobs_dict,
#     **vars(params)
# )


# Recipe parameters
# language = get_recipe_config().get("language", "")
# display_json = get_recipe_config().get("display_json", False)



# ==============================================================================
# DEFINITIONS
# ==============================================================================

# input_folder = dataiku.Folder(get_input_names_for_role("input_folder")[0])
output_dataset = dataiku.Dataset(get_output_names_for_role("output_dataset")[0])

output_dataset.write_with_schema(df)



# output_df = pd.DataFrame(results)
# output_df = df
# output_dataset.write_with_schema(output_df)
