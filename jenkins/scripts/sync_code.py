#!/usr/bin/env python3

from os import path, system
import argparse
import logging
import subprocess
import boto3
from botocore.exceptions import ClientError
from utils import jenkins_utils
from jenkins.scripts.update_batch_definition import update


def s3_sync(file_path, context, bucket_name):
    """Upload a file to an S3 bucket

       :param file_path: File to upload
       :param context: training/ inferencing
       :param bucket_name: name of the bucket to upload
       :return: True if file was uploaded, else False
    """

    basename = path.basename(file_path)

    logging.info(f"************* {basename} *************")
    logging.info(f"{bucket_name}: copy {basename} from {file_path}")

    # Upload the file
    s3_client = boto3.client('s3')
    try:
        response = s3_client.upload_file(
            file_path, bucket_name, f"src/python/{context}/{basename}")
        logging.info(f"{bucket_name}: upload {basename} complete..")
    except ClientError as e:
        logging.error(e)


def get_git_revision_short_hash() -> str:
    return subprocess.check_output(['git', 'rev-parse', '--short', 'HEAD']).decode('ascii').strip()


def ecr_sync(local_env_file, context: str, module: str, version: str, all_ecr_ssm_params, ecr_repo_prefix: str):
    ssm = boto3.client('ssm')
    ssm_param = all_ecr_ssm_params[module]
    ecr_url = f"{ecr_repo_prefix}/{name_prefix}/{context}-job/{module}:{version}"

    logging.info(f"Building new image for {module} with imageID : {ecr_url}")

    cmd = f'''
        docker build -f model/{context}_job/{module}/Dockerfile . -t {ecr_url}
        docker push {ecr_url}
    '''

    exit_code = system(cmd)
    assert exit_code == 0

    logging.info(f"Upload new {module} ECR url to SSM: {ssm_param}")
    ssm.put_parameter(Name=ssm_param, Value=ecr_url, Overwrite=True)

    if context == module:
        logging.info(
            f"For {context}: Updating batch definition SSM param required.")
        update(env_file=local_env_file, context=context, container_url=ecr_url)


def sync_eval_gatekeeper(env_data, context: str, bucket_name):

    eval_path = env_data.get(context, {}).get('evaluation_job_params', {}).get(
        'path', f"model/{context}_job/evaluation/evaluation_summary.py")
    gk_path = env_data.get(context, {}).get('gatekeeper_job_params', {}).get(
        'path', f"model/{context}_job/gatekeeper/gatekeeper.py")

    s3_sync(eval_path, context, bucket_name)
    s3_sync(gk_path, context, bucket_name)


def sync_monitoring(env_data, context: str, bucket_name):

    glue_data_quality_path = env_data.get(context, {}).get('glue_data_quality_params', {}).get(
        'path', f"model/common/{context}/data_quality_eap.py")
    glue_feature_store_path = env_data.get(context, {}).get('glue_feature_store_params', {}).get(
        'path', f"model/training_job/{context}/sample_feature_store.py")
    glue_inferencing_model_quality_path = env_data.get(context, {}).get('glue_inferencing_model_quality_params', {}).get(
        'path', f"model/inferencing_job/{context}/inference_model_quality_eap.py")
    glue_training_model_quality_path = env_data.get(context, {}).get('glue_training_model_quality_params', {}).get(
        'path', f"model/training_job/{context}/training_model_quality_eap.py")
    model_quality_threshold_path = env_data.get(context, {}).get('model_quality_threshold_path', {}).get(
        'path', f"model/common/{context}/model_quality_threshold.csv")

    s3_sync(glue_data_quality_path, context, bucket_name)
    s3_sync(glue_feature_store_path, context, bucket_name)
    s3_sync(glue_inferencing_model_quality_path, context, bucket_name)
    s3_sync(glue_training_model_quality_path, context, bucket_name)
    s3_sync(model_quality_threshold_path, context, bucket_name)


if __name__ == '__main__':
    example = '''Example:
    export PYTHONPATH="${PWD}:$PYTHONPATH" # This is to import utils
    python sync-code.py --env-file ../../env/dev.tfvars.json --context training
'''
    parser = argparse.ArgumentParser(description='Python wrapper to run sync code shell',
                                     epilog=example,
                                     formatter_class=argparse.RawDescriptionHelpFormatter)
    parser.add_argument('--env-file', type=str, required=True)
    parser.add_argument('--context', type=str, default='training')
    args = parser.parse_args()

    logging.basicConfig(
        format='%(asctime)s :: %(message)s', level=logging.INFO)

    env_file = args.env_file
    passed_context = args.context

    data = jenkins_utils.read_json(env_file)

    if passed_context == "training" or passed_context == "inferencing":
        ssm_inputs = jenkins_utils.env_to_sfn_ssm(env_file, passed_context)
        # Execute S3 Sync of Gatekeeper and Eval summary
        logging.info("S3 Sync: Gatekeeper and Evaluation summary ")

        internal_bucket = ssm_inputs['s3_bucket_name_internal']
        sync_eval_gatekeeper(data, passed_context, internal_bucket)

        logging.info("S3 Sync: Gatekeeper and Evaluation summary.. Done!")

        # Execute ECR upload of preprocessing & training/inference
        logging.info(f"ECR Upload: preprocessing & {passed_context}")

        name_prefix = jenkins_utils.get_name_prefix(env_file)
        ecr_ssm_params = jenkins_utils.get_ecr_ssm_params(
            name_prefix, passed_context)
        ecr_repo_prefix = f"{data['account_number']}.dkr.ecr.{data['region']}.amazonaws.com"

        exit_code = system(
            f"aws ecr get-login-password --region {data['region']} | docker login --username AWS --password-stdin {ecr_repo_prefix}")
        assert exit_code == 0

        git_commit_hash = get_git_revision_short_hash()

        logging.info(f"Pushing images for current commit: {git_commit_hash}")

        logging.info("ECR Upload: preprocessing")
        ecr_sync(env_file, passed_context, "preprocessing",
                 git_commit_hash, ecr_ssm_params, ecr_repo_prefix)

        logging.info(f"ECR Upload: {passed_context}")
        ecr_sync(env_file, passed_context, passed_context,
                 git_commit_hash, ecr_ssm_params, ecr_repo_prefix)

        logging.info(f"ECR Upload: preprocessing & {passed_context}.. Done!")

    if passed_context == "monitoring":
        ssm_inputs_training = jenkins_utils.env_to_sfn_ssm(
            env_file, "training")

        # Execute S3 Sync of Gatekeeper and Eval summary
        logging.info("S3 Sync: Monitoring")

        internal_bucket = ssm_inputs_training['s3_bucket_name_internal']
        sync_monitoring(data, passed_context, internal_bucket)

        logging.info("S3 Sync: Gatekeeper and Evaluation summary.. Done!")

    # All Done!!
    logging.info("All Done!!")
