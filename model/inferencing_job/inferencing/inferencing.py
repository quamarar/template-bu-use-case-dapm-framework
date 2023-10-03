import argparse
import sys
import awswrangler as wr
import joblib
import os
import boto3
import time
import logging
import s3fs
from utils.inference_dynamodb_model import (InferenceMetaDataModel,
                                            InferenceStateDataModel,
                                            InferenceAlgorithmStatus,
                                            InferenceAlgorithmS3OutputPath)
from utils.ddb_helper_functions import get_job_logstream

logger = logging.getLogger()
logger.setLevel(logging.INFO)

AWS_BATCH_JOB_ID = os.environ['AWS_BATCH_JOB_ID']
AWS_BATCH_JOB_ATTEMPT = int(os.environ["AWS_BATCH_JOB_ATTEMPT"])

args = None
algo_execution_status = []
algo_final_run_s3outputpaths = []
awsbatch_triggered_num_runs = 0
training_algo_names_global = []
inference_algo_names_global = []
overall_fail_count = 0
algo_to_model_path_mapping = []
batch_job_cw_url = ''


def get_s3_bucket_key(s3_path):
    path_parts = s3_path.replace("s3://", "").split("/")
    bucket = path_parts.pop(0)
    key = "/".join(path_parts)
    return bucket, key


def parse_args(argv):
    # print(sys_args)
    logger.info("parsing system arguments")
    parser = argparse.ArgumentParser(argv)

    parser.add_argument("--s3_inferencing_data_input_path",
                        type=str, required=True)
    parser.add_argument("--s3_inferencing_prefix_output_path",
                        type=str, required=True)
    parser.add_argument("--pk_id", type=str, required=True)
    parser.add_argument("--prev_batch_job_id", type=str, required=True)
    parser.add_argument("--inference_metatable_name", type=str, required=True)
    parser.add_argument("--mapping_id", type=str, required=True)
    parser.add_argument("--region", type=str, required=True)
    logger.info("end parse_args")
    return parser.parse_args(argv)


def clean_out_path(s3_path):
    bucket, key = get_s3_bucket_key(s3_path)
    s3 = boto3.resource('s3')
    bucket = s3.Bucket(bucket)
    bucket.objects.filter(Prefix=key).delete()


def check_and_clean_prev_batch_job_output():
    """
    If previous batch job id is not empty, implies previous batch job may have created output.
    Clean previous batch job's output
    """
    if args.prev_batch_job_id != 'empty':
        clean_out_path(args.s3_inferencing_prefix_output_path)
        logger.info(
            f"Previous batch job output path : {args.s3_inferencing_prefix_output_path} deleted")


def setup_dynamo_tables():
    """
    Setups up DynamoDB Inference Metadata and State Tables
    """
    InferenceMetaDataModel.setup_model(InferenceMetaDataModel,
                                       args.inference_metatable_name,
                                       args.region)

    inference_statetable_name = InferenceMetaDataModel.get(
        'fixedlookupkey').inference_statetable_name


    InferenceStateDataModel.setup_model(
        InferenceStateDataModel, inference_statetable_name, args.region)


def set_global_variables():
    """
    Set python global variables
    """
    if args.prev_batch_job_id != 'empty':
        item = InferenceStateDataModel.get(hash_key=args.prev_batch_job_id)
    else:
        item = InferenceStateDataModel.get(hash_key=f"{AWS_BATCH_JOB_ID}")

    global training_algo_names_global
    training_algo_names_global = item.training_algo_names

    global inference_algo_names_global
    inference_algo_names_global = item.inference_algo_names

    global awsbatch_triggered_num_runs
    awsbatch_triggered_num_runs = item.awsbatch_triggered_num_runs + 1

    # The algo here are pertaining to the specific pk , mapping id
    global algo_to_model_path_mapping
    algo_to_model_path_mapping = item.algo_to_model_input_paths_mapping
    logger.info(f"Algorithms to Model Path mapping for which inference shall run:{algo_to_model_path_mapping}")


def training_inference_algo_names_check():
    """
    check whether inference algos are subset of training algos.
    """
    if set(training_algo_names_global).issuperset(set(inference_algo_names_global)):
        logger.info("Inference Algos are subset of Training Algos")
    else:
        raise Exception(f"Inference Algos must be subset of Training Algos. "
                        f"Training Algos: {training_algo_names_global}"
                        f"Training Algos: {inference_algo_names_global}"
                        )


def setup():
    """
    Calls check routines, sets global variable and DynamoDB Tables.
    """
    check_and_clean_prev_batch_job_output()
    setup_dynamo_tables()
    set_global_variables()
    # This is being done at data gatekeeper no need to perform the check here
    # training_inference_algo_names_check()


def insert_inference_job_state_table(train_start_exec_epoc, recursive_runs=0):

    # The purpose of below sleep is to avoid contention between Glue job and Container while writing same row to Dynamo
    try:
        logger.info(f'AWS batch job id -{AWS_BATCH_JOB_ID}')
        logger.info(
            f"args.inference_metatable_name:{args.inference_metatable_name}")

        if args.prev_batch_job_id != 'empty':
            state_table_item = InferenceStateDataModel.get(
                hash_key=args.prev_batch_job_id)
        else:
            state_table_item = InferenceStateDataModel.get(
                hash_key=AWS_BATCH_JOB_ID)

        for algo_execution_status_item in algo_execution_status:
            state_table_item.algo_execution_status.append(
                algo_execution_status_item)

        state_table_item.algo_final_run_s3outputpaths = algo_final_run_s3outputpaths
        state_table_item.awsbatch_triggered_num_runs = awsbatch_triggered_num_runs
        state_table_item.last_batch_run_time = int(
            time.time()) - train_start_exec_epoc
        state_table_item.save()

    except Exception as error:
        if recursive_runs == 3:
            raise Exception(
                "Exception inside insert_inference_job_state_table")
        logger.info("Error stack during insert attempt {}".format(error))
        logger.info("Retrying insert_inference_job_state_table recursive_runs {}".format(
            recursive_runs))
        time.sleep(1)
        insert_inference_job_state_table(
            train_start_exec_epoc, recursive_runs + 1)


def append_status_path(algo, algorithm_execution_status, run_id, inference_s3_output_path):
    """
    appends inference status and output path of each algorithm
    @param inference_s3_output_path:
    @param algo:
    @param algorithm_execution_status:
    @param run_id:
    @return: None
    """
    global algo_execution_status
    algo_execution_status.append(InferenceAlgorithmStatus(algorithm_name=algo,
                                                          algorithm_execution_status=algorithm_execution_status,
                                                          runid=run_id))

    global algo_final_run_s3outputpaths
    algo_final_run_s3outputpaths.append(InferenceAlgorithmS3OutputPath(algorithm_name=algo,
                                                                       inference_s3_output_path=inference_s3_output_path
                                                                       ))


def lr(algo, model_path, preprocessed_df):
    """
    Load linear regression model, preform prediction, persist result to S3
    @return: None
    """
    try:
        if 'region' in preprocessed_df.columns:
            preprocessed_df = preprocessed_df.drop(['region', 'part_name'], axis=1)
        if 'default' in preprocessed_df.columns:
            preprocessed_df = preprocessed_df.drop(['default', 'part_name'], axis=1)

        model_tar_path = model_path

        fs = s3fs.S3FileSystem()
        with fs.open(model_tar_path, 'rb') as f:
            model = joblib.load(f)
            preprocessed_df['part_probability'] = model.predict(
                preprocessed_df)
            s3_inference_output_path = os.path.join(args.s3_inferencing_prefix_output_path,
                                                    f"batchjobid={AWS_BATCH_JOB_ID}",
                                                    f"algoname={algo}",
                                                    "prediction.parquet"
                                                    )
            wr.s3.to_parquet(df=preprocessed_df, path=s3_inference_output_path)
        append_status_path(algo=algo, algorithm_execution_status='SUCCESS', run_id=awsbatch_triggered_num_runs,
                           inference_s3_output_path=s3_inference_output_path)

    except Exception as error:
        logger.error("Inference Error for LR - {}".format(error))
        append_status_path(algo=algo, algorithm_execution_status='FAILED', run_id=awsbatch_triggered_num_runs,
                           inference_s3_output_path=s3_inference_output_path)
        global overall_fail_count
        overall_fail_count = overall_fail_count + 1


def xgb(algo, model_path, preprocessed_df):
    """
    Load linear regression model, preform prediction, persist result to S3
    @return: None
    """
    try:
        if 'region' in preprocessed_df.columns:
            preprocessed_df = preprocessed_df.drop(['region', 'part_name'], axis=1)
        if 'default' in preprocessed_df.columns:
            preprocessed_df = preprocessed_df.drop(['default', 'part_name'], axis=1)

        model_tar_path = model_path

        fs = s3fs.S3FileSystem()
        with fs.open(model_tar_path, 'rb') as f:
            model = joblib.load(f)
            preprocessed_df['part_probability'] = model.predict(
                preprocessed_df)
            s3_inference_output_path = os.path.join(args.s3_inferencing_prefix_output_path,
                                                    f"batchjobid={AWS_BATCH_JOB_ID}",
                                                    f"algoname={algo}",
                                                    "prediction.parquet"
                                                    )
            wr.s3.to_parquet(df=preprocessed_df, path=s3_inference_output_path)
        append_status_path(algo=algo, algorithm_execution_status='SUCCESS', run_id=awsbatch_triggered_num_runs,
                           inference_s3_output_path=s3_inference_output_path)

    except Exception as error:
        logger.error("Inference Error for XGB - {}".format(error))
        append_status_path(algo=algo, algorithm_execution_status='FAILED', run_id=awsbatch_triggered_num_runs,
                           inference_s3_output_path=s3_inference_output_path)
        global overall_fail_count
        overall_fail_count = overall_fail_count + 1


def main(argv):
    """
    @param argv:
    @return:
    Usage Notes :
    1. This inferencing script is a generic script for multiple algorithms.
    2. Inferening happens in a function which are abbreviated algorithm names examples : lr
    3. To extend this script, copy and paste the lr function, call the lr function in this main method
    with same check and parameter as defined for function lr
    4. Do not update any other part of the code since there are operational modules for integration with MLOps pipeline
    5. The abbreviated algorithm keys , example 'lr' , should be same as the algorithm name keys used for training
    """
    train_job_start_time = int(time.time())
    logger.info("Started Inference JOB {} ,Attempt {}".format(
        AWS_BATCH_JOB_ID, AWS_BATCH_JOB_ATTEMPT))

    global args
    args = parse_args(argv)
    time.sleep(1)
    setup()

    preprocessed_df = wr.s3.read_parquet(path=args.s3_inferencing_data_input_path)
    logger.info(preprocessed_df.groupby(['region', 'part_name']).size())

    for algo_path_mapping_object in algo_to_model_path_mapping:
        algo_name = algo_path_mapping_object['algorithm_name']
        algo_model_path = algo_path_mapping_object['inference_s3_output_path']

        if 'lr' == algo_name.lower():
            lr(algo='lr', model_path=algo_model_path, preprocessed_df=preprocessed_df.copy())

        if 'xgb' == algo_name.lower():
            xgb(algo='xgb', model_path=algo_model_path, preprocessed_df=preprocessed_df.copy())

    insert_inference_job_state_table(train_job_start_time)
    logger.info("Completed Inference JOB {} ,Attempt {}".format(
        AWS_BATCH_JOB_ID, AWS_BATCH_JOB_ATTEMPT))

    if overall_fail_count > 0:
        raise Exception("One of the algos has failed hence failing container")


if __name__ == "__main__":
    main(argv=sys.argv[1:])
