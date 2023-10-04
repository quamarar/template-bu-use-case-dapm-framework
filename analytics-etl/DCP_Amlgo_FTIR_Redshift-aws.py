"""
Note:
    1. As per the discussion with Mahendra San , output file of this job is not changed to hive style partition
    due to business reason as everytime all the files are getting processed
    Jira - https://marutide.atlassian.net/browse/M1-97
    2. Refactoring only includes paramterizing the input
"""

import awswrangler as wr
import pandas as pd
import numpy as np
import boto3
from datetime import date, datetime, timedelta
import logging
import sys
from awsglue.utils import getResolvedOptions

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
                                      "archival_path"
                                  ])

        redshift_connection_string = args['redshift_connection_string']
        input_location_path = args['input_location_path']
        stage_file_path = args['stage_file_path']
        redshift_table = args['redshift_table']
        redshift_schema = args['redshift_schema']
        archival_path = args['archival_path']


        df = wr.s3.read_csv(path=input_location_path)
        df['days'] = df['days'].astype(np.int32)
        df['mileage'] = df['mileage'].astype(np.int32)
        df = df[['product_model_code', 'segmentation', 'subject_english', 'causal_parts_no', 'date_of_incident',
                 'mileage_using_time', 'days_used', 'causal_parts_name_english', 'session', 'glue_last_updated',
                 'pmonth_year', 'Defect_resp', 'sbpr_no', 'uid', 'days', 'mileage', 'defect_response', 'base_model_code']]

        log.info("---Data Loaded----")
        table_schema = {'product_model_code': 'string',
                        'segmentation': 'string',
                        'subject_english': 'string',
                        'causal_parts_no': 'string',
                        'date_of_incident': 'timestamp',
                        'mileage_using_time': 'string',
                        'days_used': 'string',
                        'causal_parts_name_english': 'string',
                        'session': 'string',
                        'glue_last_updated': 'timestamp', 'pmonth_year': 'timestamp', 'Defect_resp': 'string',
                        'sbpr_no': 'string', 'uid': 'string',
                        'days': 'int', 'mileage': 'int', 'defect_response': 'string', 'base_model_code': 'string'}
        log.info("---Schema Extracted----")

        con = wr.redshift.connect("dcp_redshift_connect_Acc_7673")
        log.info("---Redshift Connected----")


        wr.s3.delete_objects(stage_file_path)
        log.info("stage file folder cleaning done")

  
        wr.redshift.copy(df=df, path=stage_file_path, table="ftir_data", schema="amalgo_zone", con=con,
                                 use_threads=True, mode='append', dtype=table_schema)
        log.info("--- Data moved to Redshift----")


        log.info("--- Archiving started----")
        obj = input_location_path.split("/")[-1]
        now = datetime.now() + timedelta(hours=5, minutes=30)
        suffix = now.strftime('%Y-%m-%d %H:%M:%S')
        file = obj.split(".")[0] + suffix + "." + obj.split(".")[1]
        output_path = f"{archival_path}/{file}"

        wr.s3.to_csv(df=df, path=output_path, index=False)
        wr.s3.delete_objects(input_location_path)
        log.info("--- Files Archived----")

        log.info("--- code Running End----")
    except Exception as e:
        log.info(f"Error Occurred:{e}")
        raise

