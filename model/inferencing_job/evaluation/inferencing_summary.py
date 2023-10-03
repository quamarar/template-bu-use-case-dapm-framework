import logging
import sys
from datetime import datetime
import boto3
from awsglue.utils import getResolvedOptions
import pythena
import time

from constants import SSM_INFERENCING_COMPLETE_STATUS
from ddb_helper_functions import email_sns, dump_data_to_s3, query_execution_status, update_ssm_store
from inference_dynamodb_model import InferenceMetaDataModel
import traceback

job_name = "Inference_summary"

log = logging.getLogger(__name__)
logging.basicConfig(format='%(asctime)s :: %(message)s', level=logging.INFO)
logging.info("info")

if __name__ == "__main__":
    try:
        args = getResolvedOptions(sys.argv,
                                  [
                                      'inference_metatable_name'
                                        ,'region'

                                  ])
        log.info("System Arguments {}".format(args))
        InferenceMetaDataModel.setup_model(InferenceMetaDataModel, args['inference_metatable_name'], args['region'])
        metaitem = InferenceMetaDataModel.get("fixedlookupkey")

        year = metaitem.inference_execution_year
        month = metaitem.inference_execution_month
        day = metaitem.inference_execution_day

        athenadb_metadata_table_name = metaitem.inference_athenadb_metadata_table_name
        athenadb_debug_table_name = metaitem.inference_athenadb_debug_table_name
        athena_db = metaitem.inference_athenadb_name
        region = metaitem.region
        events_client = boto3.client('events')

        inference_step_job_id = metaitem.inference_step_job_id
        eval_summary_bucket = metaitem.s3_bucket_name_shared
        object_name = """meta_inference/year={}/month={}/day={}/stepjobid={}/meta_ddb_table.json""".format(
            year, month, day, inference_step_job_id)

        # Needs to remove after adding in schema & input parameter
        # s3_preprocessing_prefix_output_path = args['s3_preprocessing_prefix_output_path']
        # training_event_bus_name = args['training_event_bus_name']
        # email_topic_arn=args['email_topic_arn']

        s3_preprocessing_prefix_output_path = metaitem.s3_preprocessing_prefix_output_path
        features_dq_input_path= metaitem.features_dq_input_path
        training_event_bus_name = metaitem.training_event_bus_name
        email_topic_arn = metaitem.email_topic_arn
        usecase_name = metaitem.inference_usecase_name

        #########################################################################################################################################
        # Dump data to s3
        dump_data_to_s3(s3_ouput_bucket=eval_summary_bucket,
                        s3_output_object_name=object_name, ddb_model=InferenceMetaDataModel)

        log.info(f"{InferenceMetaDataModel} Data dumped to {eval_summary_bucket}/{object_name}")

        #########################################################################################################################################
        # Put Event in event bridge for preprocessing data quality monitoring

        event_source = f'{usecase_name}.dataquality'
        event_detail = f"""{{"--year":"{year}",
                            "--month":"{month}",
                            "--day":"{day}",
                            "--stepjobid":"{inference_step_job_id}",
                            "--usecase_name":"{usecase_name}",
                            "--features_dq_input_path":"{features_dq_input_path}",
                            "--input_job_identifier":"inferencing",
                            "--same_account_dq_bucket": "{eval_summary_bucket}"
                            
            }}"""
        event_detail_type = 'data_quality_event'
        response = events_client.put_events(
            Entries=[
                {
                    'Time': datetime.now(),
                    'Source': event_source,
                    'DetailType': event_detail_type,
                    'Detail': event_detail,
                    'EventBusName': training_event_bus_name

                }
            ])

        log.info(f"Event generated in event bus for data quality: {response} ")

        #########################################################################################################################################
        # Athena table creation and repair
        session = boto3.session.Session()
        default_athena_client = pythena.Athena(database=athena_db, session=session, region=region)

        meta_create_query = f"""
                    CREATE EXTERNAL TABLE IF NOT EXISTS {athena_db}.{athenadb_metadata_table_name}(
                  `aws_batch_job_definition` string COMMENT 'from deserializer', 
                  `aws_batch_job_prefixname` string COMMENT 'from deserializer', 
                  `aws_batch_job_queue` string COMMENT 'from deserializer', 
                  `aws_batch_submission_timelaps` struct<end_time:int,start_time:int> COMMENT 'from deserializer', 
                  `commit_id` string COMMENT 'from deserializer', 
                  `data_quality_athenadb_name` string COMMENT 'from deserializer', 
                  `data_quality_athenadb_table_name` string COMMENT 'from deserializer', 
                  `e2e_execution_time` int COMMENT 'from deserializer', 
                  `gatekeeper_timelaps` struct<end_time:int,start_time:int> COMMENT 'from deserializer', 
                  `git_repository_url` string COMMENT 'from deserializer', 
                  `inference_athenadb_debug_table_name` string COMMENT 'from deserializer', 
                  `inference_athenadb_inference_table_name` string COMMENT 'from deserializer', 
                  `inference_athenadb_metadata_table_name` string COMMENT 'from deserializer', 
                  `inference_athenadb_name` string COMMENT 'from deserializer', 
                  `inference_execution_day` string COMMENT 'from deserializer', 
                  `inference_execution_month` string COMMENT 'from deserializer', 
                  `inference_execution_year` string COMMENT 'from deserializer', 
                  `inference_inputtable_name` string COMMENT 'from deserializer', 
                  `inference_metatable_name` string COMMENT 'from deserializer', 
                  `inference_preprocessing_prefix_input_path` string COMMENT 'from deserializer', 
                  `inference_statetable_name` string COMMENT 'from deserializer', 
                  `inference_step_job_id` string COMMENT 'from deserializer', 
                  `inference_timelaps` struct<end_time:int,start_time:int> COMMENT 'from deserializer', 
                  `inference_usecase_name` string COMMENT 'from deserializer', 
                  `input_data_set` array<string> COMMENT 'from deserializer', 
                  `features_dq_input_path` string,
                  `mapping_id_column_name` string COMMENT 'from deserializer', 
                  `mapping_json_s3_inference_path` string COMMENT 'from deserializer', 
                  `mapping_json_s3_training_path` string COMMENT 'from deserializer', 
                  `metakey` string COMMENT 'from deserializer', 
                  `pk_column_name` string COMMENT 'from deserializer', 
                  `preprocessing_timelaps` struct<end_time:int,start_time:int> COMMENT 'from deserializer', 
                  `preprocessing_total_batch_jobs` int COMMENT 'from deserializer', 
                  `region` string COMMENT 'from deserializer', 
                  `s3_bucket_name_internal` string COMMENT 'from deserializer', 
                  `s3_bucket_name_shared` string COMMENT 'from deserializer', 
                  `s3_infer_summary_prefix_output_path` string COMMENT 'from deserializer', 
                  `s3_inference_prefix_output_path` string COMMENT 'from deserializer', 
                  `state_table_total_num_batch_jobs` int COMMENT 'from deserializer', 
                  `step_function_end_time` int COMMENT 'from deserializer', 
                  `step_function_start_time` int COMMENT 'from deserializer', 
                  `total_num_batch_job_failed` int COMMENT 'from deserializer', 
                  `total_num_inference_executed` int COMMENT 'from deserializer', 
                  `total_numb_batch_job_succeeded` int COMMENT 'from deserializer', 
                  `training_algo_names` array<string> COMMENT 'from deserializer', 
                  `training_athena_pred_or_eval_table_name` string COMMENT 'from deserializer', 
                  `training_athenadb_debug_table_name` string COMMENT 'from deserializer', 
                  `training_athenadb_metadata_table_name` string COMMENT 'from deserializer', 
                  `training_athenadb_name` string COMMENT 'from deserializer', 
                  `training_execution_day` string COMMENT 'from deserializer', 
                  `training_execution_month` string COMMENT 'from deserializer', 
                  `training_execution_year` string COMMENT 'from deserializer', 
                  `training_mapping_id_column_name` string COMMENT 'from deserializer', 
                  `training_prefix_path_from_ssm` string COMMENT 'from deserializer', 
                  `training_step_job_id` string COMMENT 'from deserializer', 
                  `training_winning_algo_name` string COMMENT 'from deserializer')
                PARTITIONED BY ( 
                  `year` string, 
                  `month` string, 
                  `day` string, 
                  `stepjobid` string)
                ROW FORMAT SERDE 
                  'org.openx.data.jsonserde.JsonSerDe' 
                WITH SERDEPROPERTIES ( 
                  'paths'='aws_batch_job_definition,aws_batch_job_prefixname,aws_batch_job_queue,aws_batch_submission_timelaps,commit_id,data_quality_athenadb_name,data_quality_athenadb_table_name,e2e_execution_time,gatekeeper_timelaps,git_repository_url,inference_athenadb_debug_table_name,inference_athenadb_inference_table_name,inference_athenadb_metadata_table_name,inference_athenadb_name,inference_execution_day,inference_execution_month,inference_execution_year,inference_inputtable_name,inference_metatable_name,inference_preprocessing_prefix_input_path,inference_statetable_name,inference_step_job_id,inference_timelaps,inference_usecase_name,input_data_set,mapping_id_column_name,mapping_json_s3_inference_path,mapping_json_s3_training_path,metaKey,pk_column_name,preprocessing_timelaps,preprocessing_total_batch_jobs,region,s3_bucket_name_internal,s3_bucket_name_shared,s3_infer_summary_prefix_output_path,s3_inference_prefix_output_path,state_table_total_num_batch_jobs,step_function_end_time,step_function_start_time,total_num_batch_job_failed,total_num_inference_executed,total_numb_batch_job_succeeded,training_algo_names,training_athena_pred_or_eval_table_name,training_athenadb_debug_table_name,training_athenadb_metadata_table_name,training_athenadb_name,training_execution_day,training_execution_month,training_execution_year,training_mapping_id_column_name,training_prefix_path_from_ssm,training_step_job_id,training_winning_algo_name') 
                STORED AS INPUTFORMAT 
                  'org.apache.hadoop.mapred.TextInputFormat' 
                OUTPUTFORMAT 
                  'org.apache.hadoop.hive.ql.io.HiveIgnoreKeyTextOutputFormat'
                LOCATION
                  's3://{eval_summary_bucket}/inference/meta-inference/' 
                """
        log.info(meta_create_query)
        execution_id = default_athena_client.execute(query=meta_create_query, run_async='True')
        create_query_status = query_execution_status(execution_id, default_athena_client)

        # msck repair table meta
        meta_repair_query = f"MSCK REPAIR TABLE {athena_db}.{athenadb_metadata_table_name}"
        log.info(meta_repair_query)
        execution_id = default_athena_client.execute(query=meta_repair_query, run_async='True')
        meta_query_status = query_execution_status(execution_id, default_athena_client)

        debug_create_query = f"""
                        CREATE EXTERNAL TABLE IF NOT EXISTS {athena_db}.{athenadb_debug_table_name}(
                      `algo_execution_status` array<struct<algorithm_execution_status:string,algorithm_name:string,runid:int>> COMMENT 'from deserializer', 
                      `algo_final_run_s3_outputpaths` array<string> COMMENT 'from deserializer', 
                      `algo_to_model_input_paths_mapping` array<struct<algorithm_name:string,inference_s3_output_path:string>> COMMENT 'from deserializer', 
                      `awsbatch_job_status_overall` string COMMENT 'from deserializer', 
                      `awsbatch_triggered_num_runs` int COMMENT 'from deserializer', 
                      `batch_job_definition` string COMMENT 'from deserializer', 
                      `batch_job_status_overall` string COMMENT 'from deserializer', 
                      `batchjob_id` string COMMENT 'from deserializer', 
                      `cur_awsbatchjob_id` string COMMENT 'from deserializer', 
                      `first_run_awsbatchjob_cw_log_url` string COMMENT 'from deserializer', 
                      `inference_algo_names` array<string> COMMENT 'from deserializer', 
                      `inference_execution_day` string COMMENT 'from deserializer', 
                      `inference_execution_month` string COMMENT 'from deserializer', 
                      `inference_execution_year` string COMMENT 'from deserializer', 
                      `inference_input_data_set` array<string> COMMENT 'from deserializer', 
                      `inference_step_job_id` string COMMENT 'from deserializer', 
                      `inference_usecase_name` string COMMENT 'from deserializer', 
                      `last_batch_run_time` int COMMENT 'from deserializer', 
                      `mapping_id` string COMMENT 'from deserializer', 
                      `mapping_json_s3_inference_path` string COMMENT 'from deserializer', 
                      `mapping_json_s3_training_path` string COMMENT 'from deserializer', 
                      `num_runs` int COMMENT 'from deserializer', 
                      `pk` string COMMENT 'from deserializer', 
                      `rerun_awsbatchjob_cw_log_url` string COMMENT 'from deserializer', 
                      `rerun_awsbatchjob_id` string COMMENT 'from deserializer', 
                      `s3_eval_summary_prefix_output_path` string COMMENT 'from deserializer', 
                      `s3_inference_prefix_output_path` string COMMENT 'from deserializer', 
                      `s3_output_bucket_name` string COMMENT 'from deserializer', 
                      `s3_pk_mapping_model_prefix_input_path` string COMMENT 'from deserializer', 
                      `s3_pk_mappingid_data_input_path` string COMMENT 'from deserializer', 
                      `training_algo_names` array<string> COMMENT 'from deserializer', 
                      `training_execution_day` string COMMENT 'from deserializer', 
                      `training_execution_month` string COMMENT 'from deserializer', 
                      `training_execution_year` string COMMENT 'from deserializer', 
                      `training_step_job_id` string COMMENT 'from deserializer', 
                      `version` int COMMENT 'from deserializer', 
                      `s3_infer_summary_prefix_output_path` string COMMENT 'from deserializer', 
                      `training_winning_algo_name` string COMMENT 'from deserializer')
                    PARTITIONED BY ( 
                      `year` string, 
                      `month` string, 
                      `day` string, 
                      `stepjobid` string)
                    ROW FORMAT SERDE 
                      'org.openx.data.jsonserde.JsonSerDe' 
                    WITH SERDEPROPERTIES ( 
                      'paths'='algo_execution_status,algo_final_run_s3_outputpaths,algo_to_model_input_paths_mapping,awsbatch_job_status_overall,awsbatch_triggered_num_runs,batch_job_definition,batch_job_status_overall,batchjob_id,cur_awsbatchjob_id,first_run_awsbatchjob_cw_log_url,inference_algo_names,inference_execution_day,inference_execution_month,inference_execution_year,inference_input_data_set,inference_step_job_id,inference_usecase_name,last_batch_run_time,mapping_id,mapping_json_s3_inference_path,mapping_json_s3_training_path,num_runs,pk,rerun_awsbatchjob_cw_log_url,rerun_awsbatchjob_id,s3_eval_summary_prefix_output_path,s3_infer_summary_prefix_output_path,s3_inference_prefix_output_path,s3_output_bucket_name,s3_pk_mapping_model_prefix_input_path,s3_pk_mappingid_data_input_path,training_algo_names,training_execution_day,training_execution_month,training_execution_year,training_step_job_id,training_winning_algo_name,version') 
                    STORED AS INPUTFORMAT 
                      'org.apache.hadoop.mapred.TextInputFormat' 
                    OUTPUTFORMAT 
                      'org.apache.hadoop.hive.ql.io.HiveIgnoreKeyTextOutputFormat'
                    LOCATION
                      's3://{eval_summary_bucket}/inference/debug/' 
                    """
        log.info(debug_create_query)
        execution_id = default_athena_client.execute(query=debug_create_query, run_async='True')
        create_query_status = query_execution_status(execution_id, default_athena_client)

        # msck repair table debug
        debug_repair_query = f"MSCK REPAIR TABLE {athena_db}.{athenadb_debug_table_name}"
        log.info(debug_repair_query)
        execution_id = default_athena_client.execute(query=debug_repair_query, run_async='True')
        debug_query_status = query_execution_status(execution_id, default_athena_client)

        #########################################################################################################################################
        # Sending email

        sns_client = boto3.client('sns')
        email_subject = f"ML Inference Status:{metaitem.inference_usecase_name}"
        email_message = f"""Model Inference successfully completed for {metaitem.inference_usecase_name}
         \n Year:{metaitem.inference_execution_year} \n Month:{metaitem.inference_execution_month} \n Day:{metaitem.inference_execution_day} \n Stepjobid:{metaitem.inference_step_job_id} """
        email_sns(sns_client, email_topic_arn, email_message, email_subject)

        log.info( "Completion email sent")
    except Exception as error:
        log.error("Error ->{}".format(error))
        traceback.log.info_exc()
        sys.exit(1)