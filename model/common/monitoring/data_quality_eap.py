"""
Prerequisites
1. S3 bucket and Glue database should already be created in the EAP account.
2. Update dev parameters in this job. #TODO

Code Flow:
1. Triggered from the event( success file placed in the s3 bucket) providing the required parameters
2. Read all the preprocessing files in the same pyspark dataframe then convert into dynamic dataframe  - done
3. Input is provided which is having ruleset and the additional metric to enhance dashboard metrics
4. Ruleset is evaluated and captured the result in the same account
5. Consolidated resultset is also loaded in cross account bucket

References:
1. Working with Glue DQ in glue notebook - https://docs.aws.amazon.com/glue/latest/ug/data-quality-notebooks.html
2. Glue DQ - https://docs.aws.amazon.com/glue/latest/dg/data-quality.html
3. Data Quality definition languages - https://docs.aws.amazon.com/glue/latest/dg/dqdl.html
"""

import sys
from awsglue.transforms import *
from awsglue.utils import getResolvedOptions
from pyspark.context import SparkContext
from awsglue.context import GlueContext
from awsglue.job import Job
from awsgluedq.transforms import EvaluateDataQuality
from pyspark.sql import SparkSession
import boto3
import pythena
import time
from awsglue.dynamicframe import DynamicFrame
import logging
from dataquality_helper_functions import get_consolidated_outcomes_dq_evaluation, evaluate_only, evaluate_dq_rules, \
    gluedf_s3_loading, evaluate_dq_with_dashboard_metric, read_parquet_for_dynamic_frame, read_csv_for_dynamic_frame

log = logging.getLogger(__name__)
logging.basicConfig(format='%(asctime)s :: %(message)s', level=logging.INFO)
logging.info("info")

sc = SparkContext.getOrCreate()
glueContext = GlueContext(sc)
spark = glueContext.spark_session
job = Job(glueContext)

if __name__ == "__main__":
    args = getResolvedOptions(sys.argv,
                              [
                                  'year',
                                  'month',
                                  'day',
                                  'stepjobid',
                                  'usecase_name',
                                  'features_dq_input_path',
                                  'input_job_identifier',
                                  'eap_central_bucket',
                                  'dashboard_consoilidated_dq_tablename',
                                  'same_account_dq_bucket',
                                  'same_account_monitoring_athena_db',
                                  'job_frequency',
                                  'solution_type',
                                  'solution_category'

                              ]
                              )

    # coming from event
    year = args['year']
    month = args['month']
    day = args['day']
    stepjobid = args['stepjobid']
    usecase_name = args['usecase_name']
    preprocessing_read = args['features_dq_input_path']
    input_job_identifier = args['input_job_identifier']
    same_account_dq_bucket = args['same_account_dq_bucket']

    # coming from job parameter passed by IAC
    eap_central_bucket = args['eap_central_bucket']
    dashboard_consoilidated_dq_tablename = args['dashboard_consoilidated_dq_tablename']
    same_account_monitoring_athena_db = args['same_account_monitoring_athena_db']
    job_frequency = args['job_frequency']
    solution_type = args['solution_type']
    solution_category = args['solution_category']


    detailed_outcome_table = f"{input_job_identifier}_data_quality"

    detailed_dq_target_file_path = f's3://{same_account_dq_bucket}/{detailed_outcome_table}'

    dashboard_data_quality_path = f's3://{eap_central_bucket}/{dashboard_consoilidated_dq_tablename}/usecase_name={usecase_name}/year={year}/month={month}/day={day}/corelation_id={stepjobid}'

    ########################################################################
    # TODO BE PROVIDED BY DEV :
    """
    dq_df_dataset: List of string: Each row represents single rule and metrices to enhance for dashboard
        variablename: string: dashboard metric -Name of the columnname on which dq needs to be applied
        ruletype:     string: dashboard metric - Rule type to be shown on dashboard
        unit:         string: dashboard metric - Unit whether threshold quantity signifies number or percentage
        minthreshold: float:  dashboard metric - min threshold of the rule
        maxthreshold: float:  dashboard metric - max threshold of the rule
        rule:         string: Provide the dq rule in the glue dqdl format
                              Data Quality definition languages - https://docs.aws.amazon.com/glue/latest/dg/dqdl.html
    job_frequency: string: fexpected frequency of running this job
    """
    columns = ["variablename", "ruletype", "unit",
               "minthreshold", "maxthreshold", "rule"]
    dq_df_dataset = [
        ("part_name", "count", 'Number', 1.0, 4000.0,
         """DistinctValuesCount "part_name" between 0 and 4000"""),
        ("data_from", "count", 'Number', 1.0, 4000.0,
         """DistinctValuesCount "data_from" between 0 and 4"""),
        ("part_proababilty", "min", 'Number', 0.1, 1.0,
         """CustomSql "select min(part_probability) from primary" between 0.0 and 1.0""")
    ]

    ########################################################################
    # Reading the preprocesing file
    
    log.info("Reading input file")
    preprocessing_data_dyf = read_csv_for_dynamic_frame(
        glueContext, spark, preprocessing_read)
    # ##################################################################################################################
    # Evaluating rules for dashboard data quality
    (final_consolidated_dq_dyf, detailed_result_dyf, final_df) = evaluate_dq_with_dashboard_metric(glueContext, spark,
                                                                                                   dq_df_dataset,
                                                                                                   columns,
                                                                                                   preprocessing_data_dyf,
                                                                                                   stepjobid,
                                                                                                   input_job_identifier,
                                                                                                   year, month, day,
                                                                                                   usecase_name,
                                                                                                   job_frequency,
                                                                                                   solution_type,
                                                                                                   solution_category)

    # ##################################################################################################################
    # Write the detailed data quality result data in parquet in same account bucket
    gluedf_s3_loading(glueContext, detailed_result_dyf, detailed_dq_target_file_path, same_account_monitoring_athena_db,
                      detailed_outcome_table)

    ########################################################################
    # Write the data in parquet in cross account bucket
    log.info(dashboard_data_quality_path)

    final_df.write.mode('overwrite').parquet(
        dashboard_data_quality_path)
    log.info(
        f"Output data is written in parquet in cross account bucket: {dashboard_data_quality_path}")
