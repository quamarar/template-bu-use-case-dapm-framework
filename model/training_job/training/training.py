import argparse
import awswrangler as wr
from sklearn.linear_model import LinearRegression
from xgboost import XGBClassifier
from sklearn.model_selection import train_test_split
import joblib
import os
import boto3
import json
import time
import logging
import sys
import s3fs
import pandas as pd
import traceback
from pynamodb.attributes import UnicodeSetAttribute, NumberAttribute, ListAttribute
from utils.dynamodb_util import TrainStateDataModel, TrainingAlgorithmStatus, TrainingMetaDataModel, \
    TrainingAlgorithmS3OutputPath
from utils.ddb_helper_functions import get_job_logstream

logging.basicConfig(format='%(asctime)s %(message)s', level=logging.INFO)
logger = logging.getLogger()

AWS_BATCH_JOB_ID = os.environ['AWS_BATCH_JOB_ID']
AWS_BATCH_JOB_ATTEMPT = int(os.environ["AWS_BATCH_JOB_ATTEMPT"])

algo_execution_status = []
algo_final_run_s3outputpaths = []
awsbatch_triggered_num_runs = 0
algo_names_global = []
overall_fail_count = 0
batch_job_cw_url = ''
args = None


def clean_out_path(s3_path):
    bucket, key = get_s3_bucket_key(s3_path)
    s3 = boto3.resource('s3')
    bucket = s3.Bucket(bucket)
    bucket.objects.filter(Prefix=key).delete()


def setup(recursive_runs=0):
    try:
        logger.info(f'batch job id-{AWS_BATCH_JOB_ID}')
        TrainingMetaDataModel.setup_model(TrainingMetaDataModel,
                                          args.train_metatable_name,
                                          args.region)

        train_statetable_name = TrainingMetaDataModel.get('fixedlookupkey').train_statetable_name

        TrainStateDataModel.setup_model(TrainStateDataModel, train_statetable_name, args.region)

        if args.prev_batch_job_id != 'empty':
            item = TrainStateDataModel.get(hash_key=args.prev_batch_job_id)
        else:
            item = TrainStateDataModel.get(hash_key=f"{AWS_BATCH_JOB_ID}")
            clean_out_path(args.s3_training_prefix_output_path)
            clean_out_path(args.s3_pred_or_eval_prefix_output_path)

        for algo_name in item.algo_names:
            global algo_names_global
            algo_names_global.append(algo_name)

        global awsbatch_triggered_num_runs
        awsbatch_triggered_num_runs = item.awsbatch_triggered_num_runs + 1
        item.save()
    except Exception as error:
        logger.error(error)
        logger.error(traceback.format_exc())
        if recursive_runs == 3:
            raise Exception("Exception inside setup")
        logger.info(f"Retrying setup, reattempt count: {recursive_runs}")
        time.sleep(2)
        setup(recursive_runs + 1)


def insert_train_job_state_table(train_start_exec_epoc, recursive_runs=0):
    # try:

    # The purpose of below sleep is to avoid contention between Glue job and Container while writing same row to Dynamo
    try:

        logger.info(f"args.train_metatable_name:{args.train_metatable_name}")

        state_table_item = TrainStateDataModel.get(hash_key=AWS_BATCH_JOB_ID)
        logger.info("Updating JOBID- {} status in StateTable".format(AWS_BATCH_JOB_ID))

        for algo_execution_status_item in algo_execution_status:
            state_table_item.algo_execution_status.append(algo_execution_status_item)

        state_table_item.algo_final_run_s3outputpaths = algo_final_run_s3outputpaths
        state_table_item.awsbatch_triggered_num_runs = awsbatch_triggered_num_runs
        state_table_item.last_batch_run_time = int(time.time()) - train_start_exec_epoc

        state_table_item.save()

    except Exception as error:
        logger.error(error)
        logger.error(traceback.format_exc())
        if recursive_runs == 3:
            raise Exception("Exception inside insert_train_job_state_table")
        logger.error("Retrying insert_train_job_state_table recursive_runs {}".format(recursive_runs))
        time.sleep(1)
        insert_train_job_state_table(train_start_exec_epoc=train_start_exec_epoc, recursive_runs=recursive_runs + 1)


def get_s3_bucket_key(s3_path):
    path_parts = s3_path.replace("s3://", "").split("/")
    bucket = path_parts.pop(0)
    key = "/".join(path_parts)
    return bucket, key


def append_status_path(algo, algorithm_execution_status, run_id, s3_training_output_key, s3_evaluation_output_path):
    global algo_execution_status
    algo_execution_status.append(TrainingAlgorithmStatus(algorithm_name=algo,
                                                         algorithm_execution_status=algorithm_execution_status,
                                                         runid=run_id))

    global algo_final_run_s3outputpaths
    algo_final_run_s3outputpaths.append(TrainingAlgorithmS3OutputPath(algorithm_name=algo,
                                                                      model_s3_output_path=s3_training_output_key,
                                                                      pred_or_eval_s3_output_path=
                                                                      s3_evaluation_output_path))


def lr(algo, preprocessed_df):
    try:
        features = preprocessed_df.drop(['part_probability', 'part_name'], axis=1)
        labels = preprocessed_df['part_probability']

        # Test
        if 'region' in features.columns:
            features = features.drop(['region'], axis=1)
        if 'default' in features.columns:
            features = features.drop(['default'], axis=1)

        x_train, x_test, y_train, y_test = train_test_split(features, labels, train_size=0.9, test_size=0.1,
                                                            random_state=0)

        lr_model = LinearRegression()
        lr_model.fit(x_train, y_train)

        s3_training_prefix_output_path_updated = os.path.join(args.s3_training_prefix_output_path,
                                                              f"batchjobid={AWS_BATCH_JOB_ID}", f"algoname={algo}")
        #clean_out_path(s3_training_prefix_output_path_updated)
        logger.info("LR Saving model to {s3_training_prefix_output_path_updated}")
        s3_training_output_key = os.path.join(s3_training_prefix_output_path_updated, "model.tar.gz")

        fs = s3fs.S3FileSystem()

        with fs.open(s3_training_output_key, 'wb') as f:
            joblib.dump(lr_model, f)

        y_pred = lr_model.predict(x_test)

        y_pred_series = pd.Series(data=y_pred, name="pred")
        y_test_series = y_test.reset_index(drop=True)

        test_pred_df = pd.concat([y_test_series, y_pred_series.astype(float)], axis=1)

        #clean_out_path(args.s3_pred_or_eval_prefix_output_path)
        # check slash
        s3_evaluation_output_path = os.path.join(args.s3_pred_or_eval_prefix_output_path,
                                                 f"batchjobid={AWS_BATCH_JOB_ID}",
                                                 f"algoname={algo}",
                                                 "test_pred.parquet"
                                                 )

        wr.s3.to_parquet(df=test_pred_df, path=s3_evaluation_output_path)

        append_status_path(algo=algo,
                           algorithm_execution_status='SUCCESS',
                           run_id=awsbatch_triggered_num_runs,
                           s3_training_output_key=s3_training_output_key,
                           s3_evaluation_output_path=s3_evaluation_output_path)
    except Exception:
        logger.info("Training for LR failed ")
        logger.info(traceback.format_exc())
        append_status_path(algo=algo,
                           algorithm_execution_status='FAILED',
                           run_id=awsbatch_triggered_num_runs,
                           s3_training_output_key='',
                           s3_evaluation_output_path='')
        global overall_fail_count
        overall_fail_count = overall_fail_count + 1


def xgb(algo, preprocessed_df):
    try:
        features = preprocessed_df.drop(['part_probability', 'part_name'], axis=1)
        labels = preprocessed_df['part_probability']

        # Test
        if 'region' in features.columns:
            features = features.drop(['region'], axis=1)
        if 'default' in features.columns:
            features = features.drop(['default'], axis=1)

        x_train, x_test, y_train, y_test = train_test_split(features, labels, train_size=0.9, test_size=0.1,
                                                            random_state=0)

        xgb_model = XGBClassifier()
        xgb_model.fit(x_train, y_train)

        s3_training_prefix_output_path_updated = os.path.join(args.s3_training_prefix_output_path,
                                                              f"batchjobid={AWS_BATCH_JOB_ID}", f"algoname={algo}")
        #clean_out_path(s3_training_prefix_output_path_updated)
        logger.info("XGB  Saving model to {s3_training_prefix_output_path_updated}")
        s3_training_output_key = os.path.join(s3_training_prefix_output_path_updated, "model.tar.gz")

        fs = s3fs.S3FileSystem()

        with fs.open(s3_training_output_key, 'wb') as f:
            joblib.dump(xgb_model, f)

        y_pred = xgb_model.predict(x_test)

        y_pred_series = pd.Series(data=y_pred, name="pred")
        y_test_series = y_test.reset_index(drop=True)

        test_pred_df = pd.concat([y_test_series, y_pred_series.astype(float)], axis=1)

        #clean_out_path(args.s3_pred_or_eval_prefix_output_path)
        # check slash
        s3_evaluation_output_path = os.path.join(args.s3_pred_or_eval_prefix_output_path,
                                                 f"batchjobid={AWS_BATCH_JOB_ID}",
                                                 f"algoname={algo}",
                                                 "test_pred.parquet"
                                                 )

        wr.s3.to_parquet(df=test_pred_df, path=s3_evaluation_output_path)

        append_status_path(algo=algo,
                           algorithm_execution_status='SUCCESS',
                           run_id=awsbatch_triggered_num_runs,
                           s3_training_output_key=s3_training_output_key,
                           s3_evaluation_output_path=s3_evaluation_output_path)
    except Exception:
        logger.info("Training for XGB failed ")
        logger.info(traceback.format_exc())
        append_status_path(algo=algo,
                           algorithm_execution_status='FAILED',
                           run_id=awsbatch_triggered_num_runs,
                           s3_training_output_key='',
                           s3_evaluation_output_path='')
        global overall_fail_count
        overall_fail_count = overall_fail_count + 1


def parse_args(argv):
    parser = argparse.ArgumentParser()
    parser.add_argument("--s3_pk_mappingid_data_input_path", type=str, required=True)
    parser.add_argument("--pk_id", type=str, required=True)
    parser.add_argument("--s3_training_prefix_output_path", type=str, required=True)
    parser.add_argument("--s3_pred_or_eval_prefix_output_path", type=str, required=True)
    parser.add_argument("--prev_batch_job_id", type=str, required=True)
    parser.add_argument("--train_metatable_name", type=str, required=True)
    parser.add_argument("--mapping_id", type=str, required=True)
    parser.add_argument("--region", type=str, required=True)
    return parser.parse_args(argv)


def main(argv):
    """
    @param argv:
    @return:
    Usage Notes :
    1. This training script is a generic script.
    2. The model training happens in a function which are abbreviated algorithm names examples : lr
    3. To extend this script, copy and paste the lr function, call the lr function in this main method
    with same check and parameter as defined for function lr
    4. Do not update any other part of the code since there are operational modules for integration with MLOps pipeline
    """
    train_job_start_time = int(time.time())
    time.sleep(1)

    global args
    args = parse_args(argv)
    setup()

    logger.info("Starting Training job")
    for algo_name in algo_names_global:
        logger.info(algo_name.lower())

    preprocessed_df = wr.s3.read_parquet(path=args.s3_pk_mappingid_data_input_path)

    # For each algorithm, copy and paste below if condition and algorith function call
    # replace 'lr' with your custom algorithm name, example : xgb
    if 'lr' in (algo_name.lower() for algo_name in algo_names_global):
        lr(algo="lr", preprocessed_df=preprocessed_df.copy())

    if 'xgb' in (algo_name.lower() for algo_name in algo_names_global):
        xgb(algo="xgb", preprocessed_df=preprocessed_df.copy())

    insert_train_job_state_table(train_job_start_time)

    logger.info("Training job ending")

    if overall_fail_count > 0:
        raise Exception("One of the algos has failed hence failing container")


if __name__ == "__main__":
    main(argv=sys.argv[1:])
