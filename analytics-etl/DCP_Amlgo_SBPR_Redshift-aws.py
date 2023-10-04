"""
Note:
    1. As per the discussion with Mahendra San , output file of this job is not changed to hive style partition
    due to business reason as everytime all the files are getting processed
    Jira - https://marutide.atlassian.net/browse/M1-97
    2. Refactoring only includes paramterizing the input
"""
import awswrangler as wr
from datetime import datetime, timedelta
import logging
import sys
from awsglue.utils import getResolvedOptions

log = logging.getLogger(__name__)
logging.basicConfig(format='%(asctime)s :: %(message)s', level=logging.INFO)
logging.info("info")

input_location_path = 's3://dcp-dev-apsouth1-analytics/ProcessedData/SBPR/SBPR.csv'
redshift_connection_string = 'dcp_redshift_connect_Acc_7673'
stage_file_path = 's3://dcp-dev-apsouth1-analytics/StageFiles/TempFiles/SBPR/'
redshift_table = 'SBPR_data'
redshift_schema = 'amalgo_zone'
archival_path = 's3://dcp-dev-apsouth1-analytics/ProcessedData/SBPR'

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

        log.info("---Data Loaded----")
        table_schema = {'segmentation': 'string',
                        'product_model_code': 'string',
                        'subject_english': 'string',
                        'incident_overview_english': 'string',
                        'causal_parts_no': 'string',
                        'responsible_supplier': 'string',
                        'date_of_summary': 'timestamp',
                        'ftirs_sbpr': 'int',
                        'observation_type': 'string',
                        'category': 'string',
                        'defect_resp': 'string',
                        'causal_parts_name_english': 'string',
                        'vendor_code': 'string',
                        'session': 'string',
                        'glue_last_updated': 'timestamp', 'uid': 'string'}
        log.info("---Schema Extracted----")

        con = wr.redshift.connect(redshift_connection_string)
        log.info("---Redshift Connected----")

        wr.s3.delete_objects(stage_file_path)
        log.info("stage file folder cleaning done")

        max_len_col_dict = {'product_model_code': 1500, 'incident_overview_english': 2500}
        wr.redshift.copy(df=df, path=stage_file_path, table=redshift_table, schema=redshift_schema, con=con,
                         use_threads=True, mode='append', dtype=table_schema, varchar_lengths=max_len_col_dict)

        log.info("--- Data moved to Redshift----")
        con.close()


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
        raise e
