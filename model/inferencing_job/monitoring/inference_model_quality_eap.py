"""
Prerequisites:
1. Ensure that the inference has already been completed only once for a particular month
2. Ensure that the threshold csv is already placed in pth - 's3://msil-<usecase>-apsouth1-shared/model_quality_threshold/model_quality_threshold.csv'
3. Ensure the cross account bucket is there in EAP account
4. Provide the inputs for the #TODO sections in the code
"""

import sys
from awsglue.transforms import *
from awsglue.utils import getResolvedOptions
from pyspark.context import SparkContext
from awsglue.context import GlueContext
from awsglue.job import Job
import logging
from datetime import date

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
                                  'threshold_data_path',
                                  'usecase_name',
                                  'cross_account_bucket_name',
                                    'inference_bucket_name'])

    threshold_data_path = args['threshold_data_path']
    usecase_name = args['usecase_name']
    cross_account_bucket_name = args['cross_account_bucket_name']
    inference_bucket_name = args['inference_bucket_name']

    todays_date = date.today()
    year = todays_date.year
    month = todays_date.month
    day = todays_date.day

    month = str(month)
    len_month = len(month)

    if len_month < 2:
        month = '0' + month

    inference_read = f's3://{inference_bucket_name}/inferenceout/year={year}/month={month}/'

    # TODO - Provide the path where groundtruth data is coming
    groundtruth_read = f's3://{inference_bucket_name}/groundtruth_data/'

    ################################################################################################################################
    # TODO - Provide the evaluation logic for inferencing and capture the final output in final_df spark dataframe
    # Inference data to be captured in spark dataframe

    inference_df = spark.read.format("parquet").option("inferSchema", True).option("header", "true").load(
        inference_read)
    inference_df.createOrReplaceTempView("inference_df")

    inference_df1 = spark.sql(""" select distinct pk,mapping,algoname from 
                                        (select * , rank()over ( partition by algoname order by part_probability desc) rnk 
                                        from inference_df ) where rnk <= 200  """)

    # inference_df1.show()
    inference_df1.createOrReplaceTempView("inference_df1")

    log.info("Inference data captured in spark dataframe")

    #### Groudtruth data to be captured in spark dataframe

    groundtruth_df = spark.read.format("csv").option("inferSchema", True).option("header", "true").load(
        groundtruth_read)
    groundtruth_df.createOrReplaceTempView("groundtruth_df")

    groundtruth_df1 = spark.sql(""" select distinct partname,region from 
                                        (select * , rank()over ( partition by partname , region order by actual_failure_probabilty desc) rnk 
                                        from groundtruth_df ) where rnk <= 200  """)

    # groundtruth_df1.show()

    groundtruth_df1.createOrReplaceTempView("groundtruth_df1")

    log.info("Groundtruth data captured in spark dataframe")

    #### Final inference evaluation data to be calculated in spark dataframe
    final_df = spark.sql(""" select 'Intersection' metric_name ,count(1) metric_value ,algoname ,cast(current_timestamp as timestamp ) as audit_timestamp
                              from groundtruth_df1 inner join inference_df1
                              on inference_df1.pk = groundtruth_df1.partname 
                              and inference_df1.mapping = groundtruth_df1.region 
                              group by algoname


                         """)

    # final_df.show()
    final_df.createOrReplaceTempView("final_df")
    log.info("Final inference evaluation captured in spark dataframe")

    ########################################################################################################################################
    #### Threshold data - Reading the eval_summary_data_df
    threshold_data_df = spark.read.format("csv").option("header", "true").option("inferSchema", "true").load(
        threshold_data_path)
    log.info("Threshold data captured in spark dataframe")
    threshold_data_df.show()

    threshold_data_df.createOrReplaceTempView("threshold_data_df")

    ############################################################
    #### Final dataset for the model monitoring dashboard

    final_modelling_df = spark.sql(f"""select '{usecase_name}' as usecase_name ,a.metric_name ,unit,expected_value,metric_value as actual_value, audit_timestamp,
            '{year}' as year, '{month}' as month, '{day}' as day,  algoname,base_threshold,critical_threshold,
            case when metric_value >= base_threshold then 'Green'
                when metric_value < base_threshold and metric_value >= critical_threshold then 'Amber'
                when metric_value < critical_threshold then 'Red' end as status
            From  final_df a
            inner join threshold_data_df b on a.metric_name = b.metric_name
                 """)
    final_modelling_df.show()

    # final_modelling_dyf = DynamicFrame.fromDF(final_modelling_df, glueContext, "final_modelling_dyf")
    log.info("Final dataset captured in dynamic dataframe")

    ########################################################################
    #### Write the data in parquet in cross account bucket

    dashboard_model_quality_path = f's3://{cross_account_bucket_name}/modelquality/usecase_name={usecase_name}/year={year}/month={month}/day={day}'

    final_modelling_df.write.mode('overwrite').parquet(
        dashboard_model_quality_path)
    log.info(f"Output data is written in parquet in cross account bucket on the path {dashboard_model_quality_path}")
