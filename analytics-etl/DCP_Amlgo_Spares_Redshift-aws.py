
import awswrangler as wr
import boto3
from datetime import date
from awsglue.transforms import *
from pyspark.context import SparkContext
from awsglue.context import GlueContext
from awsglue.job import Job
import logging
import sys
from awsglue.utils import getResolvedOptions
from pyspark.sql.functions import *


sc = SparkContext()
glueContext = GlueContext(sc)
spark = glueContext.spark_session
job = Job(glueContext)


log = logging.getLogger(__name__)
logging.basicConfig(format='%(asctime)s :: %(message)s', level=logging.INFO)
logging.info("info")


if __name__ == '__main__':
    try:

        args = getResolvedOptions(sys.argv,
                                  [
                                      "redshift_connection_string",
                                      "input_location_path",
                                      "stage_file_path",
                                      "redshift_table",
                                      "redshift_schema",
                                      "iam_role_arn"
                                  ])

        redshift_connection_string = args['redshift_connection_string']
        input_location_path = args['input_location_path']
        stage_file_path = args['stage_file_path']
        redshift_table = args['redshift_table']
        redshift_schema = args['redshift_schema']
        iam_role_arn = args['iam_role_arn']
        
        
        today = date.today()
        year = today.strftime("%Y")
        month = today.strftime("%m")
        day = today.strftime("%d")

        path = f"{input_location_path}/year={year}/month={month}/day={day}/"
        
        # s3 = boto3.client('s3')

        df = spark.read.parquet(path).withColumn("sale_qty", col("sale_qty").cast("Integer")).withColumn(
            "glue_last_updated", to_timestamp("glue_last_updated"))
        log.info(f"the record count {df.count()}")

        log.info("---Data Loaded----")
        table_schema = {'invd_item_code': 'string',
                        'item_name': 'string',
                        'invoice_date': 'timestamp',
                        'sale_qty': 'int',
                        'city': 'string',
                        'state': 'string',
                        # 'invoice_date_new':'timestamp',
                        'glue_last_updated': 'timestamp', 'session': 'string'
                        }
        log.info("---Schema Extracted----")
    
        con = wr.redshift.connect(redshift_connection_string)
        log.info("---Redshift Connected----")
    
        wr.s3.delete_objects(stage_file_path)
        log.info("stage file folder cleaning done")
        
        turncate_query = f'TRUNCATE TABLE {redshift_schema}.{redshift_table};'
        df.coalesce(1).write.parquet(stage_file_path)
        with con.cursor() as cursor:
            cursor.execute(turncate_query)
        log.info("-----truncated has done------------")
        query = f"COPY {redshift_schema}.{redshift_table} FROM '{stage_file_path}' IAM_ROLE '{iam_role_arn}' FORMAT AS PARQUET;"

        with con.cursor() as cursor:
            cursor.execute(query)
        log.info("--- Data moved to Redshift----")
        con.close()
        
        log.info("--- code Running End----")
    except Exception as e:
        log.info(f"Error Occurred:{e}")
        raise e

