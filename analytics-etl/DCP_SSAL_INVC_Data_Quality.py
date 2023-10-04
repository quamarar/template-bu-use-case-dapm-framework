"""
Prerequisites:
1. Athena DB(IAC) - <usecase>_data_quality
2. Parameter to be updated in parameter store
/<usecase_name>/dev/<workflow_name>/expected_dq_job_cnt (This variable signifies the number of DQ jobs that needs to be executed in a workflow)
This is required for sending consolidated email when all the dq jobs in a glue workflow finished processing
3. Glue job should have permission to access the parameter in ssm parameter store
"""

import logging
import sys
from awsglue.transforms import *
from awsglue.utils import getResolvedOptions
from pyspark.context import SparkContext
from awsglue.context import GlueContext
from awsglue.job import Job
from awsglue.dynamicframe import DynamicFrame
import boto3
import pythena
from datetime import date, datetime
from pyspark.sql.functions import col, when

from dataquality_helper_functions import read_input_athena_table, evaluate_dq_rules, gluedf_s3_loading, \
    get_completed_dqjobs_cnt_from_worflowid, \
    get_expected_dqjob_cnt_ssm_store, get_email_message, evaluate_only, get_consolidated_outcomes_dq_evaluation, \
    after_processing_generate_dq_overall_status_consolidated, failed_processing_generate_dq_overall_status_consolidated

import dataquality_helper_functions

from ddb_helper_functions import email_sns

log = logging.getLogger(__name__)
logging.basicConfig(format=' %(job_name)s - %(asctime)s - %(message)s ')

sc = SparkContext.getOrCreate()
glueContext = GlueContext(sc)
spark = glueContext.spark_session
job = Job(glueContext)

todays_date = date.today()
year = todays_date.year
month = todays_date.month
day = todays_date.day

if __name__ == '__main__':

    try:

        ########################################################################################################################
        ### TO BE MODIFIED BY DEV : Paramters to read the source data into the glue dataframe
        """
        Required Inputs:
        source_database_name: string: Name of the athena database which has the input athena table
        source: string: Name of the athena table having input data
        column_list: string: Comma separated string of required column names
                             1. Columns on which data quality needs to be applied
                             2. Key columns which are required to identify any single record (it is needed for debugging)
        """
        column_list = 'invc_date,invc_pmod_code,invc_qty,invc_delr_code'

        evaluate_dataquality_ruleset = """
            Rules = [
    
                IsComplete "invc_pmod_code",
                IsComplete "invc_delr_code" ,
                (IsComplete "invc_qty") and (ColumnDataType "invc_qty" = "INTEGER"),
                (ColumnDataType "invc_date" = "Date") and (ColumnValues "invc_date" <= now())
    
                ] """

        ########################################################################################################################
        dq_overall_status_rule_name = 'dq_overall_status'

        log.info("Entered main")
        args = getResolvedOptions(sys.argv,
                                  ["WORKFLOW_NAME",
                                   "WORKFLOW_RUN_ID",
                                   "dq_bucket",
                                   "dq_athena_db",
                                   "consolidated_dq_table",
                                   "region",
                                   "dqjob_cnt_parameter_name",
                                   "email_ssn_topic_arn",
                                   "usecase_name",
                                   "source_database_name",
                                   "source_table",
                                   "eap_central_bucket"
                                   ])

        ##### Parameters passed as parameter through IAC
        workflow_name = args['WORKFLOW_NAME']
        corelation_id = args['WORKFLOW_RUN_ID']
        dq_bucket = args['dq_bucket']
        dq_athena_db = args['dq_athena_db']
        consolidated_dq_table = args['consolidated_dq_table']
        region = args['region']
        dqjob_cnt_parameter_name = args['dqjob_cnt_parameter_name']
        email_ssn_topic_arn = args['email_ssn_topic_arn']
        usecase_name = args['usecase_name']
        source_database_name = args['source_database_name']
        source_table = args['source_table']
        eap_central_bucket=args['eap_central_bucket']

        ########################################################################################################################
        ##### Parameters derived from provided parameters
        source = source_table
        detailed_outcome_table = source
        detailed_dq_target_file_path = f's3://{dq_bucket}/etl_data_quality/{detailed_outcome_table}'
        consolidated_dq_target_file_path = f's3://{dq_bucket}/etl_data_quality/{consolidated_dq_table}'
    
        dashboard_data_quality_path = f's3://{eap_central_bucket}/etl-data-quality/usecase_name={usecase_name}/year={year}/month={month}/day={day}/corelation_id={corelation_id}'

        boto3_session = boto3.session.Session()
        athena_client = pythena.Athena(database=dq_athena_db, session=boto3_session, region=region)
        sns_client = boto3.client('sns', region_name=region)

        ########################################################################################################################
        ##### Generating glue dataframe to be given as input for running dq checks

        input_data_dyf = read_input_athena_table(glueContext, spark, source_database_name, source, column_list)

        ########################################################################################################################
        ##### Populate the ruleset for gatekeeper DQ checks

        log.info(f"ruleset: {evaluate_dataquality_ruleset}")

        detailed_result_dyf, consolidated_dq_result_dyf = evaluate_dq_rules(glueContext, spark, input_data_dyf,
                                                                            evaluate_dataquality_ruleset, corelation_id,
                                                                            source, year, month, day)
        log.info(f"Evaluated the rules against dataset")

        ########################################################################################################################

        (consolidated_dq_result_dyf,final_dq_status) = after_processing_generate_dq_overall_status_consolidated(glueContext, spark,
                                                                                              consolidated_dq_result_dyf,
                                                                                              corelation_id,
                                                                                              dq_overall_status_rule_name,
                                                                                              year, month, day)
        log.info("Generated final dqset along with the consolidated row")

        ########################################################################################################################
        # Writing detailed data to S3 and updating glue catalog
        gluedf_s3_loading(glueContext, detailed_result_dyf, detailed_dq_target_file_path, dq_athena_db,
                          detailed_outcome_table)

        # Writing consolidated data to S3 and updating glue catalog
        gluedf_s3_loading(glueContext, consolidated_dq_result_dyf, consolidated_dq_target_file_path, dq_athena_db,
                          consolidated_dq_table)
                          
        ########################################################################
        # Write the data in parquet in cross account bucket
        
        log.info(dashboard_data_quality_path)
    
        final_df = consolidated_dq_result_dyf.toDF()
        final_df.write.mode('append').parquet(
            dashboard_data_quality_path)
        log.info(
            f"Output data is written in parquet in cross account bucket: {dashboard_data_quality_path}")

    except Exception as error:
        log.error("Error ->{}".format(error))

        try:
            job_failure_dyf = failed_processing_generate_dq_overall_status_consolidated(glueContext, spark,
                                                                                        corelation_id, source,
                                                                                        dq_overall_status_rule_name,
                                                                                        year, month, day)
            gluedf_s3_loading(glueContext, job_failure_dyf, consolidated_dq_target_file_path, dq_athena_db,
                              consolidated_dq_table)
        except Exception as error:
            log.error("Error ->{}".format(error))
            raise error
        raise error

    try:
        ########################################################################################################################
        ### Code for sending email when all dq jobs of workflow is completed

        # Fetch the number of DQ jobs which are completed for the current workflow_id
        dq_completion_job_cnt = get_completed_dqjobs_cnt_from_worflowid(athena_client, dq_athena_db,
                                                                        consolidated_dq_table, corelation_id)
        # Fetch the expected number of DQ jobs which are to be completed for sending the email
        expected_job_cnt = get_expected_dqjob_cnt_ssm_store(dqjob_cnt_parameter_name, region)

        subject = f"{usecase_name}:{workflow_name}:Data Quality Glue Job Status"
        email_message = get_email_message(athena_client, dq_athena_db, consolidated_dq_table, corelation_id)

        if int(dq_completion_job_cnt) < int(expected_job_cnt):
            log.info(f"Workflow not completed \n Following dq jobs have processing completed \n {email_message}")
        else:
            response = email_sns(sns_client, email_ssn_topic_arn, email_message, subject)

            log.info("Email sent")

        ########################################################################################################################
        ## Failing the glue job when overall dq_status is failed
        if final_dq_status == 'Failed':
            sys.exit(
                f"Final DataQuality Result for the {source_table} is {final_dq_status} in the workflow id {corelation_id}")
    except Exception as error:
        log.error("Error ->{}".format(error))
        raise error