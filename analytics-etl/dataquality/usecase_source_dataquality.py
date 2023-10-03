"""
Prerequisites:
1. Athena DB(IAC) - <usecase>_data_quality
2. Parameter to be updated in parameter store to mention the number of dq jobs 
/<usecase_name>/dev/<workflow_name>/expected_dq_job_cnt (This variable signifies the number of DQ jobs that needs to be executed in a workflow)
This is required for sending consolidated email when all the dq jobs in a glue workflow finished processing
3. Glue job should have permission to access the parameter in ssm parameter store
4. Complete the TODO section in the code
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
logging.basicConfig(format='%(asctime)s :: %(message)s', level=logging.INFO)
logging.info("info")

sc = SparkContext.getOrCreate()
glueContext = GlueContext(sc)
spark = glueContext.spark_session
job = Job(glueContext)

todays_date = date.today()
year = todays_date.year
month = todays_date.month
day = todays_date.day

if __name__ == '__main__':

    ########################################################################################################################
    ### TODO - TO BE MODIFIED BY DEV : Parameters to read the source data into the glue dataframe
    """
    Required Inputs:
    source_database_name: string: Name of the athena database which has the input athena table
    source: string: Name of the athena table having input data
    column_list: string: Comma separated string of required column names
                         1. Columns on which data quality needs to be applied
                         2. Key columns which are required to identify any single record (it is needed for debugging)
    """
    source_database_name = 'msil_datalake_curated_sr2_dblink'
    source = 'warranty_de_mwar_clm2'
    column_list = 'clm2_deind,CLM2_DELR_CD,CLM2_FOR_CD,CLM2_OUTLET_CD,CLM2_DUP_SL_NO,CLM2_ISSUE_NO,concat(clm2_deind,CLM2_DELR_CD,CLM2_FOR_CD,CLM2_OUTLET_CD,CLM2_DUP_SL_NO,CLM2_ISSUE_NO) as concat_primary'

    evaluate_dataquality_ruleset = """
        Rules = [
            IsPrimaryKey "concat_primary",
             IsComplete "clm2_deind",
             IsComplete "CLM2_DELR_CD",
             IsComplete "CLM2_FOR_CD",
             IsComplete "CLM2_OUTLET_CD",
             IsComplete "CLM2_DUP_SL_NO",
             IsComplete "CLM2_ISSUE_NO"

            ] """

    ########################################################################################################################
    dq_overall_status_rule_name = 'dq_overall_status'
    try:

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
                                   "usecase_name"
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

        ########################################################################################################################
        ##### Parameters derived from provided parameters

        detailed_outcome_table = source
        detailed_dq_target_file_path = f's3://{dq_bucket}/etl_data_quality/{detailed_outcome_table}'
        consolidated_dq_target_file_path = f's3://{dq_bucket}/etl_data_quality/{consolidated_dq_table}'

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

        consolidated_dq_result_dyf = after_processing_generate_dq_overall_status_consolidated(glueContext, spark,
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

        ########################################################################################################################
        #### Code for sending email when all dq jobs of workflow is completed

        #Fetch the number of DQ jobs which are completed for the current workflow_id
        dq_completion_job_cnt = get_completed_dqjobs_cnt_from_worflowid(athena_client, dq_athena_db, consolidated_dq_table,
        # Fetch the expected number of DQ jobs which are to be completed for sending the email
        expected_job_cnt = get_expected_dqjob_cnt_ssm_store(dqjob_cnt_parameter_name, region)

        subject = f"{usecase_name}:{workflow_name}:Data Quality Glue Job Status"
        email_message = get_email_message(athena_client, dq_athena_db, consolidated_dq_table, corelation_id)

        if int(dq_completion_job_cnt) < int(expected_job_cnt):
            log.info(f"Workflow not completed \n Following dq jobs have processing completed \n {email_message}")
        else:
            response = email_sns(sns_client, email_ssn_topic_arn, email_message, subject)

            log.info("Email sent")
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

        log.info("Ended the program cleanly")
        sys.exit(1)