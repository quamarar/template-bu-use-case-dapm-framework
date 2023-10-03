"""
Prerequisites:
1. Model registry group should be already created
2. DDB metadata table to be populated with required parameters
3. training_event_bus_name in event_bridge to be created by IAC

Code Logic:
1. Read input from the trainDataModel dynamoDB table
2. Read the parquet using spark dataframe
3. calculate the evaluation metric and write parquet in s3
4. Get the Image ARN for model registry
5. Publish the entry in model registry
6. Dump the dynamodb meta table in to s3

"""

import time
import boto3
from dynamodb_util import TrainStateDataModel, TrainingMetaDataModel, Timelaps, AlgoScore
import sys
import logging
from awsglue.transforms import *
from awsglue.utils import getResolvedOptions
from pyspark.context import SparkContext
from awsglue.context import GlueContext
from awsglue.job import Job
from ddb_helper_functions import dump_data_to_s3, email_sns, update_ssm_store, read_json_from_s3
from datetime import datetime
import pythena
import traceback

sc = SparkContext()
glueContext = GlueContext(sc)
spark = glueContext.spark_session
job = Job(glueContext)

s3_client = boto3.client('s3')


######################################################################################################
log = logging.getLogger(__name__)
logging.basicConfig(format='%(asctime)s :: %(message)s', level=logging.INFO)
logging.info("info")

def get_image_from_job_definition(job_definition_name):
    """
    Purpose: get the image id from batch_job_definition
    param : string value of job_definition_name
    return: String value containing the image uri
    """
    batch_client = boto3.client('batch')
    describe_job_id = "{}".format(job_definition_name)
    response = batch_client.describe_job_definitions(jobDefinitions=[describe_job_id])
    return response["jobDefinitions"][0]["containerProperties"]["image"]


def query_execution_status(execution_id, athena_client):
    """
    Purpose: Check the athena execution status of athena query and keep on trying if the query is in pending or running state
    param : execution_id: execution id of the executed athena query
    param : athena_client: athena client object
    return: query_status: query status of the query
    """
    query_status = athena_client.get_query_status(execution_id)
    if query_status == 'QUEUED' or query_status == 'RUNNING':
        log.info(f"Sleep for 10 seconds ")
        time.sleep(10)
        query_execution_status(execution_id, athena_client)
    elif query_status == 'SUCCEEDED':
        log.info(f"Completed ")
        return query_status
    elif query_status == 'FAILED' or query_status == 'CANCELLED':
        log.info(f"Failed ")
        return query_status


if __name__ == "__main__":
    log.info("Entered the main function")
    try:
        args = getResolvedOptions(sys.argv,
                                  [
                                      'train_inputtable_name',
                                      'train_statetable_name',
                                      'train_metatable_name',
                                      'region',
                                      'eap_central_bucket'])

        ### Captured the parameters from the passed arguments
        eap_central_bucket = args['eap_central_bucket'] 
        eval_summary_start_epoch = int(time.time())
        log.info(f"Returning the epoch time :{eval_summary_start_epoch}")

        # dynamically set the table names for input, state and meta dynamoDB tables
        TrainingMetaDataModel.setup_model(TrainingMetaDataModel, args['train_metatable_name'], args['region'])
        log.info("Instantiating dynamo table")
        metaitemtemp = TrainingMetaDataModel.get("fixedlookupkey")
        TrainStateDataModel.setup_model(TrainStateDataModel, metaitemtemp.train_statetable_name, metaitemtemp.region)

        year = metaitemtemp.execution_year
        month = metaitemtemp.execution_month
        day = metaitemtemp.execution_day
        stepjobid = metaitemtemp.step_job_id
        athena_db = metaitemtemp.athenadb_name
        athenadb_debug_table_name = metaitemtemp.athenadb_debug_table_name
        athenadb_metadata_table_name = metaitemtemp.athenadb_metadata_table_name
        eval_summary_bucket = metaitemtemp.s3_bucket_name_shared
        region = metaitemtemp.region
        usecase_name = metaitemtemp.usecase_name
        job_definition_name = metaitemtemp.aws_batch_job_definition
        training_event_bus_name = metaitemtemp.training_event_bus_name
        email_topic_arn = metaitemtemp.email_topic_arn
        
        mapping_json_constants = read_json_from_s3(metaitemtemp.mapping_json_s3_path, s3_client)
        primaryKey = mapping_json_constants["mapping_json_data"]["primary_key"]
        mapping_id = mapping_json_constants["mapping_json_data"]['Training']["mappingColumn"]

        # Parameters which are derived in this job statically
        training_output_s3_prefix = (f's3://{eval_summary_bucket}/training/year={year}/month={month}/day={day}/'
                         f'stepjobid={stepjobid}/')
        training_pred_s3_prefix = (f's3://{eval_summary_bucket}/pred_or_eval/year={year}/month={month}/day={day}/'
                         f'stepjobid={stepjobid}/')
        target_path = f"s3://{eval_summary_bucket}/model_eval_summary/"  # target S3 bucket
        eval_success_trigger_key = f'trigger_eval_summary/year={year}/month={month}/day={day}/stepjobid={stepjobid}'
        modeldataurlkey = "dummy_model/model.tar.gz"  # dummymodelpath for model registry
        object_name = """meta/year={}/month={}/day={}/stepjobid={}/meta_ddb_table.json""".format(
            year, month, day, stepjobid)

        # Need to be added in dynamo db meta table
        # model_package_group_arn = args["model_package_group_arn"]

        model_package_group_name = metaitemtemp.model_package_group_name

        commitid = metaitemtemp.commit_id
        repository = metaitemtemp.repository

        events_client = boto3.client('events')

        s3 = boto3.client('s3')
        log.info("Instantiating the boto3 session")
        sns_client = boto3.client('sns')

        ######################################################################################################
        #### Reading the preprocesing file

        training_df = spark.read.format("parquet").option("header", "true").option("inferSchema", "true").load(
            training_pred_s3_prefix)
        training_df.createOrReplaceTempView("training_df")

        actual_data_df = spark.sql(f""" select distinct pk,mapping,batchjobid,algoname from 
                                    (select * , rank()over ( partition by pk, mapping,batchjobid,algoname order by part_probability desc) rnk 
                                    from training_df ) where rnk <= 200 """)

        actual_data_df.createOrReplaceTempView("actual_data_df")

        pred_data_df = spark.sql(f""" select distinct pk,mapping,batchjobid,algoname from 
                                    (select * , rank()over ( partition by pk, mapping,batchjobid,algoname order by pred desc) rnk 
                                    from training_df ) where rnk <= 200 """)
        pred_data_df.createOrReplaceTempView("pred_data_df")

        final_df = spark.sql(f""" select 'Intersection' metric_name ,count(1) metric_value ,prd.algoname ,cast(current_timestamp as timestamp ) as audit_timestamp
                                    ,'{year}' as year , '{month}' as month , '{day}' as day , '{stepjobid}' as stepjobid
                                    from pred_data_df prd
                                    inner join actual_data_df acd
                                    on prd.pk = prd.pk and prd.mapping = acd.mapping
                                    group by prd.algoname""")
        final_df.createOrReplaceTempView("final_df")
        final_df.show(truncate=False)

        algoscore_lst = []
        df_algo_list = spark.sql("select algoname ,  metric_value from final_df")
        gen_val = df_algo_list.collect()

        for i in gen_val:
            val = AlgoScore(algo_name=i['algoname'], algo_score=float(i['metric_value']))
            log.info(val)
            algoscore_lst.append(val)

        log.info(algoscore_lst)

        #########################################################################################################################################
        # Output data is written in parquet in eval summary bucket
        eval_summary_prefix_path = """{}year={}/month={}/day={}/stepjobid={}/""".format(
            target_path, metaitemtemp.execution_year, metaitemtemp.execution_month, metaitemtemp.execution_day,
            metaitemtemp.step_job_id)
        final_df.write.mode('overwrite').parquet(eval_summary_prefix_path)
        log.info(f"Output data is written in parquet in eval summary bucket on the path {eval_summary_prefix_path} ")

        #########################################################################################################################################
        # Write the dummy model file for the model registry to enter the value
        s3.put_object(
            Bucket=eval_summary_bucket,
            Key=modeldataurlkey
        )
        #########################################################################################################################################
        # Comment: Placeholder for additional columns for model registry

        winning_model_df = spark.sql(
            """ select cast(metric_value as string) as winningalgometric, algoname as winningalgo
                from (
                select metric_value, algoname , rank() over (order by metric_value desc ) rnk from final_df
                     ) where rnk = 1 """)

        winning_algo_metric = winning_model_df.head()['winningalgometric']
        winning_algo = winning_model_df.head()['winningalgo']
        winningalgos3uri = training_output_s3_prefix

        imageurl = get_image_from_job_definition(job_definition_name)

        # need to get them image arn
        log.info(f"Starting the model registry insertion")
        modelpackage_inference_specification = {
            "InferenceSpecification": {
                "Containers": [
                    {
                        # Pass Image ARN returned from the function - get_image_from_job_definition
                        "Image": imageurl,
                        "ModelDataUrl": f"s3://{eval_summary_bucket}/{modeldataurlkey}"
                    }
                ],
                "SupportedContentTypes": ["text/csv"],
                "SupportedResponseMIMETypes": ["text/csv"],
            }
        }

        create_model_package_input_dict = {
            "ModelPackageGroupName": model_package_group_name,
            "ModelPackageDescription": f""" {{ "winning_algo":"{winning_algo}","winning_algo_metric":"{winning_algo_metric}","winningalgos3uri":"{winningalgos3uri}" }} """,

            "ModelApprovalStatus": "PendingManualApproval",

            "CustomerMetadataProperties": {
                "winning_algo": winning_algo,
                "winning_algo_metric": winning_algo_metric,
                "winningalgos3uri": winningalgos3uri},
            "MetadataProperties": {
                "CommitId": commitid,
                "GeneratedBy": "framework_evaluation",
                "ProjectId": usecase_name,
                "Repository": repository
            }
        }

        create_model_package_input_dict.update(modelpackage_inference_specification)
        log.info(create_model_package_input_dict)

        sm_client = boto3.client('sagemaker', region_name=region)

        # sm_client.create_model_package_group(ModelPackageGroupName="demo-999")

        create_model_package_response = sm_client.create_model_package(**create_model_package_input_dict)
        model_package_arn = create_model_package_response["ModelPackageArn"]
        log.info('ModelPackage Version ARN : {}'.format(model_package_arn))

        # Update MetaData Table

        metaitemtemp.s3_eval_summary_prefix_output_path = eval_summary_prefix_path
        eval_summary_end_epoch = int(time.time())
        metaitemtemp.eval_summary_timelaps = Timelaps(start_time=eval_summary_start_epoch, end_time=eval_summary_end_epoch)
        metaitemtemp.step_function_end_time = eval_summary_end_epoch
        metaitemtemp.e2e_execution_time = eval_summary_end_epoch - metaitemtemp.step_function_start_time

        metaitemtemp.algo_with_highest_score = winning_algo
        metaitemtemp.algo_score = algoscore_lst
        metaitemtemp.model_package_group_name = model_package_group_name

        metaitemtemp.save()
        log.info("Loaded the required parameters in the meta table")

        #########################################################################################################################################
        # Dump data to s3

        dump_data_to_s3(s3_ouput_bucket=eval_summary_bucket,
                        s3_output_object_name=object_name, ddb_model=TrainingMetaDataModel)
                        
        #########################################################################################################################################
        # Dump data to cross account EAP s3 bucket

        eap_prefix_key = """meta-training/usecase_name={}/year={}/month={}/day={}/stepjobid={}/meta_ddb_table.json""".format(
            usecase_name,year, month, day, stepjobid)
        
        log.info(f"Starting the data dump to cross acccount bucket to bucket :{eap_bucket} and key :{eap_prefix_key} ")
        dump_data_to_s3(s3_ouput_bucket=eap_bucket,
                        s3_output_object_name=eap_prefix_key, ddb_model=TrainingMetaDataModel)

        #########################################################################################################################################
        # Put Event in event bridge for model quality monitoring
        event_source = f'{usecase_name}.modelquality'
        event_detail = f"""{{"--year":"{year}",
                        "--month":"{month}",
                        "--day":"{day}",
                        "--stepjobid":"{stepjobid}",
                        "--usecase_name":"{usecase_name}",
                        "--eval_summary_prefix_path":"{eval_summary_prefix_path}",
                        "--input_job_identifier":"training"
        }}"""
        event_detail_type = 'model_monitoring_event'
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

        log.info(f"Event generated in event bus for model monitoring: {response} ")

        #########################################################################################################################################
        # Put Event in event bridge for preprocessing data quality monitoring
        event_source = f'{usecase_name}.dataquality'
        event_detail = f"""{{"--year":"{year}",
                            "--month":"{month}",
                            "--day":"{day}",
                            "--stepjobid":"{stepjobid}",
                            "--usecase_name":"{usecase_name}",
                            "--features_dq_input_path":"{metaitemtemp.features_dq_input_path}",
                            "--input_job_identifier":"training",
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
        # Put Event in event bridge for feature store
        event_source = f'{usecase_name}.feature_store'
        event_detail = f"""{{
                            "--stepjobid":"{stepjobid}",
                            "--features_dq_input_path":"{metaitemtemp.features_dq_input_path}",
                            "--input_job_identifier":"training"
                            
                            
            }}"""
        event_detail_type = 'feature_store_event'
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

        log.info(f"Event generated in event bus for feature store: {response} ")
        #########################################################################################################################################
        # Athena table creation and repair
        session = boto3.session.Session()
        default_athena_client = pythena.Athena(database=athena_db, session=session, region=region)

        meta_create_query = f"""
                    CREATE EXTERNAL TABLE IF NOT EXISTS {athena_db}.{athenadb_metadata_table_name}(
                  `algo_names` array<string> COMMENT 'from deserializer', 
                  `algo_score` array<struct<algo_name:string,algo_score:int>> COMMENT 'from deserializer', 
                  `algo_with_highest_score` string COMMENT 'from deserializer', 
                  `athena_pred_or_eval_table_name` string COMMENT 'from deserializer', 
                  `athenadb_debug_table_name` string COMMENT 'from deserializer', 
                  `athenadb_evaluation_summary_table` string COMMENT 'from deserializer', 
                  `athenadb_metadata_table_name` string COMMENT 'from deserializer', 
                  `athenadb_name` string COMMENT 'from deserializer', 
                  `aws_batch_job_definition` string COMMENT 'from deserializer', 
                  `aws_batch_job_prefixname` string COMMENT 'from deserializer', 
                  `aws_batch_job_queue` string COMMENT 'from deserializer', 
                  `aws_batch_submission_timelaps` struct<end_time:int,start_time:int> COMMENT 'from deserializer', 
                  `commit_id` string COMMENT 'from deserializer', 
                  `e2e_execution_time` int COMMENT 'from deserializer', 
                  `email_topic_arn` string COMMENT 'from deserializer', 
                  `eval_summary_timelaps` struct<end_time:int,start_time:int> COMMENT 'from deserializer', 
                  `execution_day` string COMMENT 'from deserializer', 
                  `execution_month` string COMMENT 'from deserializer', 
                  `execution_year` string COMMENT 'from deserializer', 
                  `gatekeeper_timelaps` struct<end_time:int,start_time:int> COMMENT 'from deserializer', 
                  `input_data_set` array<string> COMMENT 'from deserializer', 
                  `mapping_id_column_name` string COMMENT 'from deserializer', 
                  `mapping_json_s3_path` string COMMENT 'from deserializer', 
                  `metakey` string COMMENT 'from deserializer', 
                  `model_creation_pred_or_eval_timelaps` struct<end_time:int,start_time:int> COMMENT 'from deserializer', 
                  `model_package_group_name` string COMMENT 'from deserializer', 
                  `pk_column_name` string COMMENT 'from deserializer', 
                  `preprocessing_timelaps` struct<end_time:int,start_time:int> COMMENT 'from deserializer', 
                  `preprocessing_total_batch_jobs` int COMMENT 'from deserializer', 
                  `region` string COMMENT 'from deserializer', 
                  `repository` string COMMENT 'from deserializer', 
                  `s3_bucket_name_internal` string COMMENT 'from deserializer', 
                  `s3_bucket_name_shared` string COMMENT 'from deserializer', 
                  `s3_eval_summary_prefix_output_path` string COMMENT 'from deserializer', 
                  `s3_pred_or_eval_prefix_output_path` string COMMENT 'from deserializer', 
                  `s3_preprocessing_prefix_output_path` string COMMENT 'from deserializer', 
                  `s3_training_prefix_output_path` string COMMENT 'from deserializer', 
                  `state_table_total_num_batch_jobs` int COMMENT 'from deserializer', 
                  `step_function_end_time` int COMMENT 'from deserializer', 
                  `step_function_start_time` int COMMENT 'from deserializer', 
                  `step_job_id` string COMMENT 'from deserializer', 
                  `total_num_batch_job_failed` int COMMENT 'from deserializer', 
                  `total_num_models_created` int COMMENT 'from deserializer', 
                  `total_numb_batch_job_succeeded` int COMMENT 'from deserializer', 
                  `train_inputtable_name` string COMMENT 'from deserializer', 
                  `train_metatable_name` string COMMENT 'from deserializer', 
                  `train_statetable_name` string COMMENT 'from deserializer', 
                  `training_event_bus_name` string COMMENT 'from deserializer', 
                  `usecase_name` string COMMENT 'from deserializer',
                  `s3_bucket_name_analytics_etl` string)
                  
                PARTITIONED BY ( 
                  `year` string, 
                  `month` string, 
                  `day` string, 
                  `stepjobid` string)
                ROW FORMAT SERDE 
                  'org.openx.data.jsonserde.JsonSerDe' 
                WITH SERDEPROPERTIES ( 
                  'paths'='algo_names,algo_score,algo_with_highest_score,athena_pred_or_eval_table_name,athenadb_debug_table_name,athenadb_evaluation_summary_table,athenadb_metadata_table_name,athenadb_name,aws_batch_job_definition,aws_batch_job_prefixname,aws_batch_job_queue,aws_batch_submission_timelaps,commit_id,e2e_execution_time,email_topic_arn,eval_summary_timelaps,execution_day,execution_month,execution_year,gatekeeper_timelaps,input_data_set,mapping_id_column_name,mapping_json_s3_path,metaKey,model_creation_pred_or_eval_timelaps,model_package_group_name,pk_column_name,preprocessing_timelaps,preprocessing_total_batch_jobs,region,repository,s3_bucket_name_internal,s3_bucket_name_shared,s3_eval_summary_prefix_output_path,s3_pred_or_eval_prefix_output_path,s3_preprocessing_prefix_output_path,s3_training_prefix_output_path,state_table_total_num_batch_jobs,step_function_end_time,step_function_start_time,step_job_id,total_num_batch_job_failed,total_num_models_created,total_numb_batch_job_succeeded,train_inputtable_name,train_metatable_name,train_statetable_name,training_event_bus_name,usecase_name') 
                STORED AS INPUTFORMAT 
                  'org.apache.hadoop.mapred.TextInputFormat' 
                OUTPUTFORMAT 
                  'org.apache.hadoop.hive.ql.io.HiveIgnoreKeyTextOutputFormat'
                LOCATION
                  's3://{eval_summary_bucket}/meta/' 
                """
        log.info(meta_create_query)
        execution_id = default_athena_client.execute(query=meta_create_query, run_async='True')
        create_query_status = query_execution_status(execution_id, default_athena_client)
        
        
        debug_create_query = f"""
                    CREATE EXTERNAL TABLE IF NOT EXISTS {athena_db}.{athenadb_debug_table_name}(
                 `algo_execution_status` array<struct<algorithm_execution_status:string,algorithm_name:string,runid:int>> COMMENT 'from deserializer', 
                  `algo_final_run_s3outputpaths` array<struct<algorithm_name:string,model_s3_output_path:string,pred_or_eval_s3_output_path:string>> COMMENT 'from deserializer', 
                  `algo_names` array<string> COMMENT 'from deserializer', 
                  `awsbatch_job_status_overall` string COMMENT 'from deserializer', 
                  `awsbatch_triggered_num_runs` int COMMENT 'from deserializer', 
                  `batch_job_definition` string COMMENT 'from deserializer', 
                  `batchjob_id` string COMMENT 'from deserializer', 
                  `cur_awsbatchjob_id` string COMMENT 'from deserializer', 
                  `first_run_awsbatchjob_cw_log_url` string COMMENT 'from deserializer', 
                  `input_data_set` array<string> COMMENT 'from deserializer', 
                  `last_batch_run_time` int COMMENT 'from deserializer', 
                  `mapping_id` string COMMENT 'from deserializer', 
                  `mapping_json_s3_path` string COMMENT 'from deserializer', 
                  `num_runs` int COMMENT 'from deserializer', 
                  `pk` string COMMENT 'from deserializer', 
                  `rerun_awsbatchjob_cw_log_url` string COMMENT 'from deserializer', 
                  `rerun_awsbatchjob_id` string COMMENT 'from deserializer', 
                  `s3_eval_summary_prefix_output_path` string COMMENT 'from deserializer', 
                  `s3_pk_mappingid_data_input_path` string COMMENT 'from deserializer', 
                  `s3_pred_or_eval_prefix_output_path` string COMMENT 'from deserializer', 
                  `s3_training_prefix_output_path` string COMMENT 'from deserializer', 
                  `step_job_id` string COMMENT 'from deserializer', 
                  `usecase_name` string COMMENT 'from deserializer', 
                  `version` int COMMENT 'from deserializer')
                PARTITIONED BY ( 
                  `year` string, 
                  `month` string, 
                  `day` string, 
                  `stepjobid` string)
                ROW FORMAT SERDE 
                  'org.openx.data.jsonserde.JsonSerDe' 
                WITH SERDEPROPERTIES ( 
                  'paths'='algo_execution_status,algo_final_run_s3outputpaths,algo_names,awsbatch_job_status_overall,awsbatch_triggered_num_runs,batch_job_definition,batchjob_id,cur_awsbatchjob_id,first_run_awsbatchjob_cw_log_url,input_data_set,last_batch_run_time,mapping_id,mapping_json_s3_path,num_runs,pk,rerun_awsbatchjob_cw_log_url,rerun_awsbatchjob_id,s3_eval_summary_prefix_output_path,s3_pk_mappingid_data_input_path,s3_pred_or_eval_prefix_output_path,s3_training_prefix_output_path,step_job_id,usecase_name,version') 
                STORED AS INPUTFORMAT 
                  'org.apache.hadoop.mapred.TextInputFormat' 
                OUTPUTFORMAT 
                  'org.apache.hadoop.hive.ql.io.HiveIgnoreKeyTextOutputFormat'
                LOCATION
                  's3://{eval_summary_bucket}/debug/' 
                """
        log.info(debug_create_query)
        execution_id = default_athena_client.execute(query=debug_create_query, run_async='True')
        create_query_status = query_execution_status(execution_id, default_athena_client)
        

        # msck repair table meta
        debug_repair_query = f"MSCK REPAIR TABLE {athena_db}.{athenadb_debug_table_name}"
        log.info(debug_repair_query)
        execution_id = default_athena_client.execute(query=debug_repair_query, run_async='True')
        debug_query_status = query_execution_status(execution_id, default_athena_client)

        # msck repair table debug
        meta_repair_query = f"MSCK REPAIR TABLE {athena_db}.{athenadb_metadata_table_name}"
        log.info(meta_repair_query)
        execution_id = default_athena_client.execute(query=meta_repair_query, run_async='True')
        meta_query_status = query_execution_status(execution_id, default_athena_client)

        #########################################################################################################################################
        # Success email
        email_subject = f"ML Training Status:{usecase_name}"
        email_message = f"Model Training successfully completed for {usecase_name} \n Year:{year} \n Month:{month} \n Day:{day} \n Stepjobid:{stepjobid} "
        email_sns(sns_client, email_topic_arn, email_message, email_subject)
    except Exception as error:
        log.error("Error ; {}".format(error))
        traceback.log.info_exc()
        raise error

