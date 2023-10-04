"""
Comment:
1. Bucket and data folder name removed from the config file and passed in the code through IAC (terraform)
"""
from awsglue.transforms import *
from pyspark.context import SparkContext
from awsglue.context import GlueContext
from awsglue.job import Job
import logging
from delta import *
from pyspark.sql.window import *
from pyspark.sql import *
from pyspark.sql.functions import *
from pyspark.sql.types import *
from awsglue.dynamicframe import DynamicFrame
import sys
from awsglue.utils import getResolvedOptions

log = logging.getLogger(__name__)
logging.basicConfig(format='%(asctime)s :: %(message)s', level=logging.INFO)
logging.info("info")

sc = SparkContext.getOrCreate()
glueContext = GlueContext(sc)
spark = glueContext.spark_session
job = Job(glueContext)

spark.conf.set("spark.sql.legacy.timeParserPolicy", "LEGACY")
import datetime
from datetime import date, timedelta

log.info("--- code Running Started----")
import pandas as pd
import awswrangler as wr
from datetime import date, datetime, timedelta
import boto3
import numpy as np

log.info("--- Libraries Loaded----")

#######Creating dynamodb and s3 client.
dynamodb = boto3.resource('dynamodb', region_name='ap-south-1')
s3 = boto3.client('s3')
paginator = s3.get_paginator('list_objects_v2')

today = date.today()
year = today.strftime("%Y")
month = today.strftime("%m")
day = today.strftime("%d")


def getdata(prcs, mode, bucket, data_folder):
    try:
        df = DeltaTable.forPath(spark, prcs['accesspoint'])
        df = df.toDF()
        log.info(f'Table Name:{prcs["table"]} and Count:', df.count())
        data_loc = f's3://{bucket}/{data_folder}/CuratedLayer/{prcs["table"]}/'
        log.info(f' the file location {data_loc}')
        wr.s3.delete_objects(data_loc)
        glue_df = DynamicFrame.fromDF(df, glueContext, "test_nest")
        sink = glueContext.getSink(connection_type="s3", path=data_loc, enableUpdateCatalog=True,
                                   updateBehavior="UPDATE_IN_DATABASE")
        sink.setFormat("glueparquet")
        sink.setCatalogInfo(catalogDatabase=prcs['catalog_db'], catalogTableName=prcs["table"])
        sink.writeFrame(glue_df)
        log.info('*****************--------***********')

    except Exception as e:
        log.info(f'Error:{e}')
    else:
        log.info(f'Table {prcs["table"]} Retrival Successful')


def getsocialmediafiles(prcs, mode, bucket, data_folder):
    try:
        data_loc = f's3://{bucket}/{data_folder}/CuratedLayer/{prcs["table"]}/'
        obj_list = wr.s3.list_objects(prcs['accesspoint'])
        obj_list = [obj for obj in obj_list if obj.split(".")[-1] in ('parquet', 'csv', 'xlsx', 'zip')]
        wr.s3.delete_objects(data_loc)
        wr.s3.copy_objects(paths=obj_list, source_path=prcs['accesspoint'], target_path=data_loc)
        log.info(f"the {prcs['table']} files loaded to {data_loc}")

    except Exception as e:
        log.info(f'Error:{e}')
    else:
        log.info(f'Table {prcs["table"]} Retrival Successful')


def getmanualfiles(prcs, bucket, data_folder):
    link_list = prcs['accesspoint'].split("/", 3)
    curation_bucket = link_list[2]
    curation_prefix = link_list[-1]

    # log.info(f"link_list : {link_list}\n curation_bucket : {curation_bucket} \n curation_prefix: {curation_prefix}")
    current_date = datetime.now().date()
    list_last_added = []
    file_type = ['csv', 'xlsx', 'zip']
    data_loc = f's3://{bucket}/{data_folder}/CuratedLayer/{prcs["table"]}/'
    log.info(
        f'Paginating the source path files on bucket : {curation_bucket} with prefix : {curation_prefix}\n link_list : {link_list}')
    pages = paginator.paginate(Bucket=curation_bucket, Prefix=curation_prefix)
    for page in pages:
        if page["KeyCount"] == 0:
            objs = []
        else:
            objs = page['Contents']
            prcs_date = current_date - timedelta(days=prcs['days_to_retrive'])
            log.info(f"prcs_date: {prcs_date} calcualted with no of days difference:{prcs['days_to_retrive']}")
            obj_list = [f"s3://{curation_bucket}/{obj['Key']}" for obj in objs if
                        obj["LastModified"].date() > prcs_date and obj["Key"].split(".")[-1] in file_type]
            log.info(f"Lastmodified file of files to be copied : {obj_list}")
            wr.s3.delete_objects(data_loc)
            log.info(f"Files deleted on location : {data_loc}")
            wr.s3.copy_objects(paths=obj_list, source_path=prcs['accesspoint'], target_path=data_loc)
            log.info(f"the {prcs['table']} files loaded to {data_loc} from the path : {obj_list}")


if __name__ == '__main__':

    try:
        args = getResolvedOptions(sys.argv,
                                  [
                                      "config_path",
                                      "bucket",
                                      "data_folder"
                                  ])
        config_path = args['config_path']
        bucket = args['bucket']
        data_folder = args['data_folder']

        log.info('start execution')
        config_df = spark.read.csv(config_path, header=True, inferSchema=True).coalesce(1)
        log.info('Reading the dataframe from the config files')
        config_df.show()
        config_df = config_df.rdd.collect()
        log.info('df read')
        for prcs in config_df:
            if (prcs['category'] == 'master' or prcs['category'] == 'transaction'):
                log.info(f'Master file function call')
                getdata(prcs, 'overwrite', bucket, data_folder)
            elif prcs['category'] == 'manual' and prcs["table"] == 'Social_Media':
                log.info(prcs["table"])
                getsocialmediafiles(prcs, 'overwrite', bucket, data_folder)
            elif prcs['category'] == 'manual':
                log.info(f'Manual file function call')
                getmanualfiles(prcs, bucket, data_folder)

    except Exception as e:
        log.info(f'Error:{e}')
        raise e