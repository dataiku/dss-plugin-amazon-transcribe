# -*- coding: utf-8 -*-
"""Module with utility classes for validating and loading plugin parameters"""

import logging
from typing import List, Dict, AnyStr
from enum import Enum

import pandas as pd
from fastcore.utils import store_attr

import dataiku
from dataiku.customrecipe import get_recipe_config, get_input_names_for_role, get_output_names_for_role

from plugin_io_utils import ErrorHandling
from plugin_io_utils import PATH_COLUMN
from dku_io_utils import generate_path_df

from amazon_transcribe_api_client import AWSTranscribeAPIWrapper

from dku_constants import SUPPORTED_LANGUAGES
from dku_constants import SUPPORTED_AUDIO_FORMATS
from dku_constants import SUPPORTED_PII_TYPES

# TODO
DOC_URL = "https://www.dataiku.com/product/plugins/.../"


class RecipeID(Enum):
    """Enum class to identify each recipe"""

    TRANSCRIBE = "transcribe"


class PluginParamValidationError(ValueError):
    """Custom exception raised when the plugin parameters chosen by the user are invalid"""

    pass


class PluginParams:
    """Class to hold plugin parameters"""

    def __init__(
            self,
            aws_access_key_id: str,
            aws_secret_access_key: str,
            aws_session_token: str,
            aws_region_name: str,
            max_attempts: str,
            input_df: pd.DataFrame,
            input_folder: dataiku.Folder,
            column_prefix: AnyStr = "api",
            input_folder_is_s3: bool = True,
            input_folder_bucket: AnyStr = "",
            input_folder_root_path: AnyStr = "",
            output_dataset: dataiku.Dataset = None,
            output_folder: dataiku.Folder = None,
            output_folder_is_s3: bool = True,
            output_folder_bucket: AnyStr = "",
            output_folder_root_path: AnyStr = "",
            language: AnyStr = "auto",
            display_json: bool = False,
            show_speaker_labels: bool = False,
            max_speaker_labels: int = 0,
            redact_pii: bool = False,
            pii_types: AnyStr = "",
            timeout_min: int = 120,
            use_timeout: bool = True,
            parallel_workers: int = 4,
            **kwargs,
    ):
        store_attr()


class PluginParamsLoader:
    """Class to validate and load plugin parameters"""

    def __init__(self, recipe_id: RecipeID):
        self.recipe_id = recipe_id
        self.column_prefix = self.recipe_id.value
        self.recipe_config = get_recipe_config()
        self.batch_support = False  # Changed by `validate_input_params` if input folder is on GCS

    def validate_input_params(self) -> Dict:
        """Validate input parameters"""
        input_params = {}
        input_folder_names = get_input_names_for_role("input_folder")
        if len(input_folder_names) == 0:
            raise PluginParamValidationError("Please specify input folder")
        input_params["input_folder"] = dataiku.Folder(input_folder_names[0])

        file_extensions = SUPPORTED_AUDIO_FORMATS
        input_params["input_df"] = generate_path_df(
            folder=input_params["input_folder"], file_extensions=file_extensions, path_column=PATH_COLUMN
        )
        input_folder_type = input_params["input_folder"].get_info().get("type", "")
        input_params["input_folder_is_s3"] = input_folder_type == "S3"
        if input_params["input_folder_is_s3"]:
            input_folder_access_info = input_params["input_folder"].get_info().get("accessInfo", {})
            input_params["input_folder_bucket"] = input_folder_access_info.get("bucket")
            input_params["input_folder_root_path"] = str(input_folder_access_info.get("root", ""))[1:]
            logging.info("Input folder is stored on S3")
        else:
            logging.info(f"Input folder is not stored on S3 ({input_folder_type})")
            raise PluginParamValidationError("Input folder not stored on S3")
        return input_params

    def validate_output_params(self) -> Dict:
        """Validate output parameters"""
        output_params = {}
        # Output dataset
        output_dataset_names = get_output_names_for_role("output_dataset")
        if len(output_dataset_names) == 0:
            raise PluginParamValidationError("Please specify output dataset")
        output_params["output_dataset"] = dataiku.Dataset(output_dataset_names[0])

        # Optional output folder
        output_folder_names = get_output_names_for_role("output_folder")
        if len(output_folder_names) == 0:
            output_params["output_folder"] = None
        else:
            output_params["output_folder"] = dataiku.Folder(output_folder_names[0])

            output_folder_type = output_params["output_folder"].get_info().get("type", "")
            output_params["output_folder_is_s3"] = output_folder_type == "S3"
            if output_params["output_folder_is_s3"]:
                output_folder_access_info = output_params["output_folder"].get_info().get("accessInfo", {})
                output_params["output_folder_bucket"] = output_folder_access_info.get("bucket")
                output_params["output_folder_root_path"] = str(output_folder_access_info.get("root", ""))[1:]
                logging.info("Output folder is stored on S3")
            else:
                logging.info(f"Output folder is not stored on S3 ({output_folder_type})")
                raise PluginParamValidationError("Output folder not stored on S3")

        return output_params


    def validate_preset_params(self) -> Dict:
        """Validate API configuration preset parameters"""
        preset_params = {}
        api_configuration_preset = self.recipe_config.get("api_configuration_preset", {})
        if not api_configuration_preset:
            raise PluginParamValidationError(f"Please specify an API configuration preset according to {DOC_URL}")
        preset_params["aws_access_key_id"] = api_configuration_preset.get("aws_access_key_id")
        preset_params["aws_secret_access_key"] = api_configuration_preset.get("aws_secret_access_key")
        preset_params["aws_session_token"] = api_configuration_preset.get("aws_session_token")
        preset_params["aws_region_name"] = api_configuration_preset.get("aws_region_name")
        preset_params["max_attempts"] = api_configuration_preset.get("max_attempts")

        if not api_configuration_preset.get("parallel_workers"):
            raise PluginParamValidationError(f"Please specify concurrency in the preset according to {DOC_URL}")
        preset_params["parallel_workers"] = int(api_configuration_preset.get("parallel_workers"))
        if preset_params["parallel_workers"] < 1 or preset_params["parallel_workers"] > 100:
            raise PluginParamValidationError("Concurrency must be between 1 and 100")

        preset_params_displayable = {
            param_name: param_value
            for param_name, param_value in preset_params.items()
            if param_name not in {"aws_access_key_id", "aws_secret_access_key", "aws_session_token", "api_wrapper"}
        }
        logging.info(f"Validated preset parameters: {preset_params_displayable}")
        return preset_params

    def validate_recipe_params(self) -> Dict:
        """Validate recipe parameters"""
        recipe_params = {}

        if "language" in self.recipe_config:
            language = self.recipe_config["language"]
            if language not in SUPPORTED_LANGUAGES and language != "":
                raise PluginParamValidationError({f"Invalid language code: {language}"})
            recipe_params["language"] = language

        if "display_json" in self.recipe_config:
            recipe_params["display_json"] = self.recipe_config["display_json"]

        recipe_params["use_timeout"] = "timeout_min" in self.recipe_config
        recipe_params["timeout_min"] = None
        if recipe_params["use_timeout"]:
            if "timeout_min" not in self.recipe_config:
                raise PluginParamValidationError({f"Timeout has to be set"})
            elif self.recipe_config["timeout_min"] < 0:
                raise PluginParamValidationError({f"Timeout has to be larger than zero"})
            else:
                recipe_params["timeout_min"] = self.recipe_config["timeout_min"]
        
        if "show_speaker_labels" in self.recipe_config:
            recipe_params["show_speaker_labels"] = "show_speaker_labels" in self.recipe_config
            if "max_speaker_labels" not in self.recipe_config:
                raise PluginParamValidationError({f"Number of speakers has to be set"})
            elif self.recipe_config["max_speaker_labels"] < 1:
                raise PluginParamValidationError({f"Speakers has to be larger than one"})
            else:
                recipe_params["max_speaker_labels"] = self.recipe_config["max_speaker_labels"]

        if "redact_pii" in self.recipe_config:
            recipe_params["redact_pii"] = "redact_pii" in self.recipe_config
            if recipe_params["redact_pii"]:
                pii_types = self.recipe_config["pii_types"]
                if pii_types == "":
                    recipe_params["pii_types"]='ALL'
                elif len(set(pii_types).intersection(SUPPORTED_PII_TYPES)) > 0 and pii_types != "":
                    raise PluginParamValidationError({f"Invalid PII type"})
                else:
                    recipe_params["pii_types"] = pii_types
                        
        logging.info(f"Validated recipe parameters: {recipe_params}")
        return recipe_params

    def validate_load_params(self) -> PluginParams:
        """Validate and load all parameters into a `PluginParams` instance"""
        input_params = self.validate_input_params()
        output_params = self.validate_output_params()
        preset_params = self.validate_preset_params()
        recipe_params = self.validate_recipe_params()

        if output_params['output_folder'] is None:
            output_params['output_folder'] = input_params['input_folder']
            output_params["output_folder_bucket"] = input_params["input_folder_bucket"]
            output_params["output_folder_root_path"] = input_params["input_folder_root_path"]

        plugin_params = PluginParams(
            batch_support=self.batch_support,
            column_prefix=self.column_prefix,
            **input_params,
            **output_params,
            **recipe_params,
            **preset_params,
        )
        return plugin_params
