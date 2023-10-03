# function to update ssm parameter with aws batch arn
import boto3
import logging
import json


def read_ssm_parameter(ssm_param_name):
    ssm = boto3.client('ssm')
    response_ssm = ssm.get_parameter(
        Name=ssm_param_name,
        WithDecryption=True
    )
    return response_ssm


def get_sfn_ssm_inputs_param_name(prefix, context):
    ssm_sfn_param_name = f"/{prefix}-dapf-ssm/{context}_step_function_inputs"
    return ssm_sfn_param_name


def get_batch_ssm_params(prefix, context):
    ssm_batch_param_name = f"/{prefix}-dapf-ssm/{context}/live_{context}_batch_job_definition"
    return ssm_batch_param_name


def get_ecr_ssm_params(prefix, context):
    ssm_ecr_params = {
        "preprocessing": f"/{prefix}-dapf-ssm/{context}/ecr_preprocessing",
        context: f"/{prefix}-dapf-ssm/{context}/ecr_{context}"
    }
    return ssm_ecr_params


def read_json(env_file):
    with open(env_file) as json_file:
        data = json.load(json_file)

    logging.info("************** Env File ***************")
    logging.info(data)

    return data


def get_name_prefix(env_file):
    data = read_json(env_file)

    region_short = (data['region']).replace("-", "")
    name_prefix = f"{data['use_case_name']}-{data['env']}-{region_short}"

    return name_prefix


def env_to_sfn_ssm(env_file, context):
    name_prefix = get_name_prefix(env_file)

    ssm_inputs = json.loads(read_ssm_parameter(
        get_sfn_ssm_inputs_param_name(name_prefix, context))['Parameter']['Value'])

    return ssm_inputs
