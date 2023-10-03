"""
Feature store group definition:
1. feature group name
2. storage type - offline /online/both
3. s3 location where to store the data
"""

from pyspark.sql import SparkSession
from feature_store_pyspark.FeatureStoreManager import FeatureStoreManager
from awsglue.utils import getResolvedOptions
import sys
import logging
import boto3
import pandas as pd
import sagemaker
from sagemaker.feature_store.feature_group import FeatureGroup
from sagemaker.session import Session
import time


sts = boto3.client('sts')

log = logging.getLogger(__name__)
logging.basicConfig(format='%(asctime)s :: %(message)s', level=logging.INFO)
logging.info("info")

def wait_while_feature_group_created(feature_group):
    status = feature_group.describe().get("FeatureGroupStatus")
    while status == "Creating":
        print("Waiting for Feature Group to be Created")
        time.sleep(2)
        status = feature_group.describe().get("FeatureGroupStatus")
    if status != 'Created':
        raise Exception("Failed to create feature group... ")


if __name__ == "__main__":
    args = getResolvedOptions(sys.argv,
                              [
                                'feature_store_role_arn',
                                'eap_central_bucket',
                              'features_dq_input_path',
                              'stepjobid',
                              'input_job_identifier'
                               ])

    feature_store_role_arn = args['feature_store_role_arn']
    feature_store_input_path = args['features_dq_input_path']
    stepjobid = args['stepjobid']
    input_job_identifier = args['input_job_identifier']
    eap_central_bucket = args['eap_central_bucket']
    eap_bucket_prefix = 'xxxx-dapm-apsouth1-feature-store-xxxx-xxxx' # replace the prefix with you logical name
    
    response = sts.assume_role(RoleArn=feature_store_role_arn, 
                            RoleSessionName='EapFeatureStoreCrossAccountAccess')
    access_key_id = response['Credentials']['AccessKeyId']
    secret_access_key = response['Credentials']['SecretAccessKey']
    session_token = response['Credentials']['SessionToken']
    
    sagemaker_client = boto3.client('sagemaker', 
                                 aws_access_key_id=access_key_id,
                                 aws_secret_access_key=secret_access_key,
                                 aws_session_token=session_token)
                                 
    sagemaker_session = sagemaker.Session(sagemaker_client=sagemaker_client)
    
    ########################################################################################################################
    # code to get the dataframe to be loaded in the feature store
    spark = SparkSession.builder.getOrCreate()
    input_feature_dataframe = spark.read.option("inferSchema", True).option("header", True).csv(
        feature_store_input_path)
        
    log.info(f"Read the dataframe from the path {feature_store_input_path}")

    input_feature_dataframe.createOrReplaceTempView("input_feature_dataframe")
    final_input_feature_dataframe = spark.sql("""select nvl(base_model,'') as base_model ,nvl(engine_type,'') as engine_type,nvl(vdtl_tm_type,'') as vdtl_tm_type ,nvl(vdtl_fuel,'') as vdtl_fuel ,nvl(platform,'') as platform,nvl(defect_count,0) as defect_count,nvl(part_probability,0) as part_probability,
                 nvl(data_from,'') as data_from,nvl(part_name,'') as part_name, cast(current_timestamp() as string)   as event_time
                 from input_feature_dataframe""")
    ## load schema
    final_input_feature_dataframe.printSchema()
    pandasDF = final_input_feature_dataframe.toPandas()
    # pandasDF['event_time'] = pd.to_datetime(pandasDF['event_time'],unit='ms',origin='unix')
    pandasDF['event_time'] = pandasDF['event_time'].astype(str)
    
    log.info(pandasDF.dtypes)
    
    log.info(f"Dataframe created with required paramters")

    ########################################################################################################################
    feature_group_name = "xxxx-dapm-apsouth1-feature-store-xxxx-xxxxxxxx" # replace the prefix with you logical name
    try: 
        feature_group_response = sagemaker_client.describe_feature_group(FeatureGroupName = feature_group_name)
    except Exception as error:
            log.error(f"Error on creating feature_group: {error}")
            log.info(f"{feature_group_name}: Feature group not found in feature store...")
            log.info(f"Creating feature_group:{feature_group_name}")
            feature_group = FeatureGroup(
                name=feature_group_name, sagemaker_session=sagemaker_session
            )   
            

            feature_group.load_feature_definitions(data_frame=pandasDF)
            feature_group.create(
                    s3_uri=f"s3://{eap_central_bucket}/{eap_bucket_prefix}",
                    record_identifier_name='part_name',
                    event_time_feature_name="event_time",
                    role_arn=feature_store_role_arn,
                    enable_online_store=False)
            wait_while_feature_group_created(feature_group)
            feature_group_response = sagemaker_client.describe_feature_group(FeatureGroupName=feature_group_name)
    
    feature_group_arn = feature_group_response['FeatureGroupArn']

    # feature_store_input_path= "s3://msil-poc-apsouth1-internal/pre_processing/year=2023/month=08/day=07/stepjobid=S123/"

    # Initialize FeatureStoreManager with a role arn if your feature group is created by another account
    feature_store_manager = FeatureStoreManager(feature_store_role_arn)
    log.info(f"feature_store_manager created with role :{feature_store_role_arn}")

    # Load the feature definitions from input schema. The feature definitions can be used to create a feature group
    # feature_definitions = feature_store_manager.load_feature_definitions_from_schema(df)

    feature_store_manager.ingest_data(input_data_frame=final_input_feature_dataframe,
                                      feature_group_arn=feature_group_arn, target_stores=["OfflineStore"])

    log.info(f"""Loaded data from {feature_store_input_path} to feature store {feature_group_arn}\n
              stepjobid:{stepjobid} \n
              input_job_identifier:{input_job_identifier}
              """)