import logging
import sys
import time
from datetime import datetime
import re
import pythena

import boto3
from awsglue.utils import getResolvedOptions


from ddb_helper_functions import (copy_mapping_json, read_json_from_s3, read_athena_table_data,
                                  get_algo_set_of_training_and_inferencing, email_sns,
                                  get_mapping_column_of_training_and_inferencing, update_ssm_store)
from inference_dynamodb_model import InferenceMetaDataModel, Timelaps
import traceback
# from ddb_helper_functions import copy_mapping_json
# from inference_dynamodb_model import  InferenceMetaDataModel,Timelaps

logger = logging.getLogger()
logger.setLevel(logging.INFO)


# TODO - sns
# TODO - git url needs to saved

def populate_training_meta_table(args, training_meta_data, train_path_component):
    try:
        step_job_id_post_split = "-".join(args['execution_id'].split(":")[-2:])

        InferenceMetaDataModel(hash_key="fixedlookupkey",
                               inference_usecase_name=args['use_case_name'],
                               inference_execution_year=args['year'],
                               inference_execution_month=args['month'],
                               inference_execution_day=args['day'],

                               training_execution_year=train_path_component['year'],
                               training_execution_month=train_path_component['month'],
                               training_execution_day=train_path_component['day'],

                               aws_batch_job_definition=args['aws_batch_job_definition_arn'],
                               aws_batch_job_queue=args['aws_batch_job_queue'],
                               aws_batch_job_prefixname=args['aws_batch_job_name'],
                               s3_bucket_name_shared=args['s3_bucket_name_shared'],
                               s3_bucket_name_internal=args['s3_bucket_name_internal'],

                               inference_inputtable_name=args['inference_inputtable_name'],
                               inference_metatable_name=args['inference_metatable_name'],
                               inference_statetable_name=args['inference_statetable_name'],

                               training_prefix_path_from_ssm=train_path_component[
                                   'approved_model_prefix_path'],
                               training_step_job_id=train_path_component['stepjobid'],

                               mapping_json_s3_inference_path=args['mapping_json_S3_inference_path'],
                               mapping_json_s3_training_path=training_meta_data['mapping_json_s3_path'][
                                   'fixedlookupkey'],
                               commit_id=str(training_meta_data['commit_id'][
                                   'fixedlookupkey']),
                               inference_athenadb_name=args['inference_athenadb_name'],
                               training_athenadb_name=training_meta_data['athenadb_name']['fixedlookupkey'],
                               training_athenadb_debug_table_name=training_meta_data['athenadb_debug_table_name'][
                                   'fixedlookupkey'],
                               training_athenadb_metadata_table_name=training_meta_data['athenadb_metadata_table_name'][
                                   'fixedlookupkey'],
                               training_athena_pred_or_eval_table_name=training_meta_data[
                                   'athena_pred_or_eval_table_name']['fixedlookupkey'],
                               inference_athenadb_debug_table_name=args['inference_athenadb_debug_table_name'],
                               inference_athenadb_metadata_table_name=args[
                                   'inference_athenadb_metadata_table_name'],
                               inference_athenadb_inference_table_name=args[
                                   'inference_athenadb_inference_table_name'],
                               region=args['region'],
                               inference_step_job_id=step_job_id_post_split,
                               training_event_bus_name=args['training_event_bus_name'],
                               email_topic_arn=args['email_topic_arn'],
                               s3_bucket_name_analytics_etl=args['s3_bucket_name_analytics_etl']

                               ).save()
    except Exception as error:
        logger.error("Error while inserting in InferenceInputTable- {}".format(error))
        raise Exception("populate_inference_meta_table failed")


def read_ssm_store(ssm_parameter_name, arguments) -> dict:
    logger.info("fetch ssm parameter value- {}".format(ssm_parameter_name))
    ssm_client = boto3.client('ssm', region_name=arguments['region'])

    response = ssm_client.get_parameter(
        Name=ssm_parameter_name,
    )
    return response


def extract_components_from_s3_path(s3_path):
    # Regular expression patterns for year, month, day, and stepjobid
    year_pattern = r'year=(\d{4})'
    month_pattern = r'month=(\d{2})'
    day_pattern = r'day=(\d{2})'
    stepjobid_pattern = r'stepjobid=([^/]+)'

    # Search for patterns in the S3 path
    year_match = re.search(year_pattern, s3_path)
    month_match = re.search(month_pattern, s3_path)
    day_match = re.search(day_pattern, s3_path)
    stepjobid_match = re.search(stepjobid_pattern, s3_path)

    components = {
        'year': year_match.group(1) if year_match else None,
        'month': month_match.group(1) if month_match else None,
        'day': day_match.group(1) if day_match else None,
        'stepjobid': stepjobid_match.group(1) if stepjobid_match else None,
        'approved_model_prefix_path': s3_path
    }

    return components


if __name__ == '__main__':
    try:

        logger.info("Inference GateKeeper has Started...")
        args = getResolvedOptions(sys.argv,
                                  [
                                      'execution_id',
                                      'use_case_name',
                                      'year',
                                      'month',
                                      'day',
                                      'aws_batch_job_definition_arn',
                                      'aws_batch_job_queue',
                                      'aws_batch_job_name',
                                      's3_bucket_name_shared',
                                      's3_bucket_name_analytics_etl',
                                      's3_bucket_name_internal',
                                      'inference_inputtable_name',
                                      'inference_statetable_name',
                                      'inference_metatable_name',
                                      'inference_athenadb_name',
                                      'inference_athenadb_debug_table_name',
                                      'inference_athenadb_metadata_table_name',
                                      'training_athenadb_name',
                                      'training_athenadb_metadata_table_name',
                                      'mapping_json_S3_inference_path',
                                      'ssm_inferencing_complete_status',
                                      'ssm_approved_model_prefix_path',
                                      'region',
                                      'inference_athenadb_inference_table_name',
                                      'email_topic_arn',
                                      'ssm_winning_algo_name',
                                      'dq_athena_db',
                                      'dq_table',
                                      'training_event_bus_name'
                                  ])

        logger.info("System Arguments=>  {}".format(args))

        gatekeeper_start_epoch = int(time.time())

        dq_athena_db = args['dq_athena_db']
        dq_table = args['dq_table']
        region = args['region']

        usecase_name = args['use_case_name']
        email_topic_arn = args['email_topic_arn']
        step_job_id = "-".join(args['execution_id'].split(":")[-2:])
        s3_client = boto3.client('s3')
        sns_client = boto3.client('sns')

        session = boto3.session.Session()
        athena_client = pythena.Athena(
            database=dq_athena_db, session=session, region=region)

        # s3://msil-poc-apsouth1-shared/training/year=2023/month=08/day=21/stepjobid=MSILStateMachine:3bc7b47d-0fbf-4224-92d1-023604bd18fb
        approved_model_prefix_path = read_ssm_store(
            args['ssm_approved_model_prefix_path'], args)['Parameter']['Value']
        training_winning_algo_name = read_ssm_store(
            args['ssm_winning_algo_name'], args)['Parameter']['Value']

        path_components = extract_components_from_s3_path(
            approved_model_prefix_path)

        logger.info("Training Year:", path_components['year'])
        logger.info("Training Month:", path_components['month'])
        logger.info("Training Day:", path_components['day'])
        logger.info("Training Stepjobid:", path_components['stepjobid'])

        training_year = path_components['year']
        training_month = path_components['month']
        training_day = path_components['day']
        training_stepjobid = path_components['stepjobid']

        InferenceMetaDataModel.setup_model(
            InferenceMetaDataModel, args['inference_metatable_name'], args['region'])
        if not InferenceMetaDataModel.exists():
            InferenceMetaDataModel.create_table(
                read_capacity_units=100, write_capacity_units=100)
            time.sleep(10)

        #################### Gatekeeper Logic goes in #########################

        # msck repair table
        # dq_repair_query = f"MSCK REPAIR TABLE {dq_athena_db}.{dq_table}"
        # logger.info(dq_repair_query)
        # execution_id = athena_client.execute(query=dq_repair_query, run_async='True')
        # query_status = query_execution_status(execution_id, athena_client)

        # DQ failure status check
        athena_dq_failure_check = f""" select count(1) as failed_count from (
                                select * , rank() over (partition by source , rule order by audittimestamp desc ) rnk
                                from {dq_athena_db}.{dq_table}  where rule = 'dq_overall_status' ) where rnk = 1 and outcome = 'Failed'"""
        logger.info(f"athena_dq_failure_check :{athena_dq_failure_check}")
        (validation_df, execution_id) = athena_client.execute(
            query=athena_dq_failure_check)
        fail_cnt = validation_df['failed_count'].tolist()[0]

        # fail_cnt = 0
        logger.info(f"ETL DQ Failure count :  {fail_cnt}")
        if fail_cnt > 0:
            dq_email_message = f'{datetime.now()}-> Alert! {fail_cnt} DQ checks for {usecase_name} ETL failed, unable to proceed with ML processing'
            dq_email_subject = f'{usecase_name} ML Model : DQ Status Update'
            response = email_sns(sns_client, email_topic_arn,
                                 dq_email_message, dq_email_subject)
            logger.info("Sent Email")
            raise Exception(
                f'{datetime.now()}-> Alert! {fail_cnt} DQ checks for {usecase_name} ETL failed, unable to proceed with ML processing')

        #################### Gatekeeper Logic ends here #########################

        # fetching training meta table  dump

        training_meta_table_athena_query = """select * from {}.{} where year ='{}'and month ='{}'
        and day='{}' and step_job_id ='{}'""".format(args['training_athenadb_name'], args['training_athenadb_metadata_table_name'],
                                                     training_year, training_month, training_day, training_stepjobid)

        meta_table_df = read_athena_table_data(
            athena_db_name=args['training_athenadb_name'],
            region_name=args['region'],
            db_query=training_meta_table_athena_query
        )
        logger.info("metatabledata ->{}".format(meta_table_df.shape[0]))
        if meta_table_df.shape[0] == 1:
            meta_table_df.set_index('metakey', inplace=True)
            meta_table_dic = meta_table_df.to_dict()
            mapping_json_S3_train_path = meta_table_dic['mapping_json_s3_path']['fixedlookupkey']
        else:
            logger.error(
                "Meta table is supposed to have only one record with metakey as fixedlookupkey")
            raise Exception("Meta table should have exactly 1 record")

        # checking train algos superset of Inference algos
        inference_json = read_json_from_s3(
            args['mapping_json_S3_inference_path'])
        training_json = read_json_from_s3(mapping_json_S3_train_path)
        train_algo_inference, infer_algos_inference = get_algo_set_of_training_and_inferencing(
            inference_json)
        train_algo_from_training, infer_algos_from_training = get_algo_set_of_training_and_inferencing(
            training_json)

        if not train_algo_from_training.issuperset(infer_algos_inference):
            logger.error(
                "inference algo are not subset of training algo..Kindly use same set of algo")
            raise Exception("inference algo are not subset of training algo")

        (_, inference_mapping_column) = get_mapping_column_of_training_and_inferencing(
            inference_json)
        (train_mapping_column, _) = get_mapping_column_of_training_and_inferencing(
            training_json)

        logger.info(f"train_mapping_column ->{train_mapping_column}")

        logger.info(f"inference_mapping_column ->{inference_mapping_column}")

        if not train_mapping_column in [inference_mapping_column, 'default']:
            email_message = f'{datetime.now()}-> Alert!  mapping json integrity checks for {usecase_name} and stepjobid {step_job_id} failed, unable to proceed with ML processing'
            email_subject = f'{usecase_name} ML Model : Mapping Json Integrity check Failure at Training'
            email_sns(sns_client, email_topic_arn,
                      email_message, email_subject)
            raise Exception(
                "inference mapping column is not subset of training mapping column")


        #######################################################################
        'Updating inference complete in ssm'
        update_ssm_store(
            ssm_parameter_name=args['ssm_inferencing_complete_status'], value='False')
        populate_training_meta_table(args, meta_table_dic, path_components)

        # execution_id example is ARN  - arn:aws:states:ap-south-1:ACCNO:execution:MSILStateMachine:0a260727-cfea-407d-adfa-dc5add684217

        inference_source_s3_mapping_json_path = args['mapping_json_S3_inference_path']

        logger.info("Shared and interal  S3 bucket, incoming mapping_json_s3_path :{} ,{}, {} ".format(
            args['s3_bucket_name_shared'],
            args['s3_bucket_name_internal'], inference_source_s3_mapping_json_path))

        shared_s3_mapping_json_key = "inference/mappingjson/year={}/month={}/day={}/stepjobid={}/mapping.json".format(
            args['year'],
            args['month'],
            args['day'],
            step_job_id)

        mapping_json_constants, mapping_destination_path = copy_mapping_json(
            source_path=inference_source_s3_mapping_json_path,
            destination_s3_bucket=args['s3_bucket_name_shared'],
            destination_s3_key=shared_s3_mapping_json_key)
        logger.info(mapping_json_constants)
        logger.info("*********** Loading mapping json from S3: Complete ! ************")

        mapping_json_s3_path = args['s3_bucket_name_shared'] + \
                               shared_s3_mapping_json_key
        logger.info("Mapping Json S3 path private copy is : {} ".format(
            mapping_json_s3_path))

        primaryKey = mapping_json_constants["mapping_json_data"]["primary_key"]

        mapping_id = mapping_json_constants["mapping_json_data"]['Inference']["mappingColumn"]

        # Populate other elements of the Meta Table
        meta_item = InferenceMetaDataModel.get(hash_key="fixedlookupkey")
        meta_item.step_function_start_time = gatekeeper_start_epoch
        meta_item.pk_column_name = primaryKey
        meta_item.mapping_id_column_name = mapping_id
        gatekeeper_end_epoch = int(time.time())
        meta_item.step_function_start_time = gatekeeper_start_epoch
        meta_item.gatekeeper_timelaps = Timelaps(start_time=gatekeeper_start_epoch,
                                                 end_time=gatekeeper_end_epoch)
        meta_item.training_algo_names = (
            meta_table_dic['algo_names']['fixedlookupkey']).strip('][').split(',')
        # TODO  Needs to be fixed
        # inferencing json
        meta_item.mapping_json_s3_path = mapping_destination_path
        meta_item.training_winning_algo_name = training_winning_algo_name
        meta_item.training_mapping_id_column_name = train_mapping_column

        meta_item.save()

        # Revalidate deserialization of Model is happening as expected
        test_item = InferenceMetaDataModel.get(hash_key="fixedlookupkey")
        logger.info("Dumped metadata table entryu is => {}".format(
            test_item.to_json()))
    except Exception as error:
        logger.error("Error ->{}".format(error))
        traceback.print_exc()
        sys.exit(1)
