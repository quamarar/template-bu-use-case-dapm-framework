"""
Prerequisite:
1. S3 bucket and Athena DB(dashboard_monitoring_db) for dashboard should be created in EAP ( cross account)

Code Flow:
1. Place the model quality thresold on the bucket s3://msil-<usecase>-apsouth1-shared/model_quality_threshold/
2. Joined the threshold dataframe with the model_eval_summary datafra,e to get the required data for year , month, day and stepjobid
3. Load the data in cross account bucket using glue dynamic dataframe

"""

import sys
from awsglue.transforms import *
from awsglue.utils import getResolvedOptions
from pyspark.context import SparkContext
from awsglue.context import GlueContext
from awsglue.job import Job
from pyspark.sql import SparkSession
from awsglue.utils import getResolvedOptions
from awsglue.dynamicframe import DynamicFrame
import logging

sc = SparkContext.getOrCreate()
glueContext = GlueContext(sc)
spark = glueContext.spark_session
job = Job(glueContext)

log = logging.getLogger(__name__)
logging.basicConfig(format='%(asctime)s :: %(message)s', level=logging.INFO)
logging.info("info")

if __name__ == "__main__":
    args = getResolvedOptions(sys.argv,
                              [
                                  'year',
                                  'month',
                                  'day',
                                  'stepjobid',
                                  'usecase_name',
                                  'eval_summary_prefix_path',
                                  'threshold_data_path',
                                  'eap_central_bucket',
                                  'model_quality_prefix'

                              ]
                              )

    # coming from event
    year = args['year']
    month = args['month']
    day = args['day']
    stepjobid = args['stepjobid']
    usecase_name = args['usecase_name']
    input_read_path = args['eval_summary_prefix_path']

    # coming from IAC
    threshold_data_path = args['threshold_data_path']
    eap_central_bucket = args['eap_central_bucket']
    model_quality_prefix = args['model_quality_prefix']



    ########################################################################
    #### Input data - Reading the eval_summary_data_df
    eval_summary_data_df = spark.read.format("parquet").option("header", "true").option("inferSchema", "true").load(
        input_read_path)
    eval_summary_data_df.show()

    eval_summary_data_df.createOrReplaceTempView("eval_summary_data_df")

    log.info("Read the input data into spark dataframe")

    ########################################################################
    #### Threshold data - Reading the eval_summary_data_df
    threshold_data_df = spark.read.format("csv").option("header", "true").option("inferSchema", "true").load(
        threshold_data_path)
    log.info("Threshold data captured in spark dataframe")
    threshold_data_df.show()

    threshold_data_df.createOrReplaceTempView("threshold_data_df")

    ############################################################
    #### Final dataset for the model monitroing dashboard

    final_modelling_df = spark.sql(f"""select a.metric_name ,unit,expected_value,metric_value as actual_value, audit_timestamp,
            '{stepjobid}' as stepjobid, algoname,base_threshold,critical_threshold,
            case when metric_value >= base_threshold then 'Green'
                when metric_value < base_threshold and metric_value >= critical_threshold then 'Amber'
                when metric_value < critical_threshold then 'Red' end as status , 'Training' as source
            From  eval_summary_data_df a
            inner join threshold_data_df b on a.metric_name = b.metric_name
                 """)
    final_modelling_df.show()

    final_modelling_dyf = DynamicFrame.fromDF(final_modelling_df, glueContext, "final_modelling_dyf")
    log.info("Final dataset captured in dynamic dataframe")

    ########################################################################
    #### Write the data in parquet in cross account bucket
    dashboard_model_quality_path = f's3://{eap_central_bucket}/{model_quality_prefix}/usecase_name={usecase_name}/year={year}/month={month}/day={day}'
    
    final_modelling_df.write.mode('overwrite').parquet(
        dashboard_model_quality_path)
    log.info(f"Output data is written in parquet in cross account bucket on the path {dashboard_model_quality_path}")