#!/usr/bin/env python3

import argparse
import json
import logging
from datetime import date
from utils import jenkins_utils
import boto3


def execute_step_function(sfn_arn, input_data):
    stepfunctions = boto3.client('stepfunctions')

    response_running_sfn = stepfunctions.list_executions(
        stateMachineArn=sfn_arn,
        statusFilter='RUNNING',
    )

    assert len(response_running_sfn['executions']) < 1, "There are existing running executions!!"

    response_sfn = stepfunctions.start_execution(
        stateMachineArn=sfn_arn,
        input=input_data
    )
    return response_sfn


def build_sfn_input(ssm_data):
    today = date.today()

    year = today.strftime("%Y")
    month = today.strftime("%m")
    day = today.strftime("%d")

    generated_params = {
        'year': year,
        'month': month,
        'day': day,
    }

    return {**ssm_data, **generated_params}


if __name__ == '__main__':
    example = '''Example:
    export PYTHONPATH="${PWD}:$PYTHONPATH" # This is to import utils
    python step_function_executioner.py --env-file ../../env/jenkins.tfvars.json
'''
    parser = argparse.ArgumentParser(description='Execute step function passing parameters from json env',
                                     epilog=example,
                                     formatter_class=argparse.RawDescriptionHelpFormatter)
    parser.add_argument('--env-file', type=str, required=True)
    parser.add_argument('--context', type=str, default='training')
    args = parser.parse_args()

    logging.basicConfig(format='%(asctime)s :: %(message)s', level=logging.INFO)

    ssm_inputs = jenkins_utils.env_to_sfn_ssm(args.env_file, args.context)

    logging.info("************** sfn_inputs ***************")
    sfn_input = build_sfn_input(ssm_inputs)
    logging.info(sfn_input)

    logging.info(f"************** Executing {args.context} step function ***************")
    response = execute_step_function(
        sfn_arn=sfn_input['step_function_arn'], input_data=json.dumps(sfn_input))

    logging.info(response)

    logging.info("Done!")
