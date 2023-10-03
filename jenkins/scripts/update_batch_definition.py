#!/usr/bin/env python3

import argparse
import logging
import boto3
from utils import jenkins_utils


def update_batch(job_definition, container_url):
    batch = boto3.client("batch")

    response_jd = batch.describe_job_definitions(
        jobDefinitionName=job_definition
    )

    sorted_response_jd = sorted(response_jd['jobDefinitions'], key=lambda x: x['revision'], reverse=True)

    logging.info(f"Current job definition version: {sorted_response_jd[0]['jobDefinitionArn']}")
    logging.info(f"Current image url: {sorted_response_jd[0]['containerProperties']['image']}")

    logging.info(f'New container image URL: {container_url}')
    sorted_response_jd[0]["containerProperties"]["image"] = container_url

    if container_url != "NOURL":
        response_rjd = batch.register_job_definition(
            jobDefinitionName=job_definition,
            type=sorted_response_jd[0]["type"],
            containerProperties=sorted_response_jd[0]["containerProperties"],
            tags=sorted_response_jd[0]["tags"]
        )
    else:
        response_rjd = sorted_response_jd[0]

    logging.info(f"De-registering old definitions.")

    for jd in sorted_response_jd:
        delete_jd_arn = jd['jobDefinitionArn']
        if jd['status'] == "ACTIVE":
            batch.deregister_job_definition( jobDefinition=delete_jd_arn)

    return response_rjd


# function to update ssm parameter with aws batch arn
def update_ssm_parameter(parameter_name, parameter_value):
    ssm = boto3.client("ssm")

    response_p = ssm.put_parameter(
        Name=parameter_name,
        Value=parameter_value,
        Type='String',
        Overwrite=True
    )
    return response_p


def update(env_file, context, container_url):
    name_prefix = jenkins_utils.get_name_prefix(env_file)

    batch_job_definition = f"{name_prefix}-dapf-batch-{context}-job-def"

    # get job definition name from arg parser and update ssm param.
    # Also deletes inactive old definitions of container_url NOURL
    response_batch = update_batch(batch_job_definition, container_url)

    if container_url != "NOURL":
        batch_def_ssm_param_name = jenkins_utils.get_batch_ssm_params(name_prefix, context)

        logging.info(f"Batch Job definition SSM Param {batch_def_ssm_param_name}")
        logging.info("Updating SSM..")

        update_ssm_parameter(batch_def_ssm_param_name, response_batch["jobDefinitionArn"])

        logging.info(
            f'New version of batch job definition {response_batch["jobDefinitionArn"]} published in ssm param {batch_job_definition}')


if __name__ == '__main__':
    example = '''Example:
    export PYTHONPATH="${PWD}:$PYTHONPATH" # This is to import utils
    Update batch job definition and SSM ==>
    python update_batch_definition.py --context training --env-file env/dev.tfvars.json --container_url ECR_IMAGE_URL

    Just delete old definitions:
    python update_batch_definition.py --context training --env-file env/dev.tfvars.json
'''
    parser = argparse.ArgumentParser(description='Update batch definition image url and update to ssm parameter',
                                     epilog=example,
                                     formatter_class=argparse.RawDescriptionHelpFormatter)
    parser.add_argument('--container_url', type=str, required=False, default="NOURL")
    parser.add_argument('--env-file', type=str, required=True)
    parser.add_argument('--context', type=str, default='training')

    args = parser.parse_args()
    logging.basicConfig(format='%(asctime)s %(message)s', level=logging.INFO)

    update(args.env_file, args.context, args.container_url)

    logging.info('Done!!')
