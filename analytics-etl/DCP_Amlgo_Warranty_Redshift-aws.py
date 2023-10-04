import awswrangler as wr
from datetime import date, datetime, timedelta
import logging
import sys
from awsglue.utils import getResolvedOptions

log = logging.getLogger(__name__)
logging.basicConfig(format='%(asctime)s :: %(message)s', level=logging.INFO)
logging.info("info")

input_location_path = 's3://dcp-dev-apsouth1-analytics/ProcessedData/Warranty'
redshift_connection_string = 'dcp_redshift_connect_Acc_7673'
stage_file_path = 's3://dcp-dev-apsouth1-analytics/StageFiles/TempFiles/warranty/'
redshift_table = 'warranty_data_master'
redshift_schema = 'amalgo_zone'

if __name__ == '__main__':
    try:

        args = getResolvedOptions(sys.argv,
                                  [
                                      "redshift_connection_string",
                                      "input_location_path",
                                      "stage_file_path",
                                      "redshift_table",
                                      "redshift_schema"
                                  ])

        redshift_connection_string = args['redshift_connection_string']
        input_location_path = args['input_location_path']
        stage_file_path = args['stage_file_path']
        redshift_table = args['redshift_table']
        redshift_schema = args['redshift_schema']


        today = date.today()
        year = today.strftime("%Y")
        month = today.strftime("%m")
        day = today.strftime("%d")

        df = wr.s3.read_parquet(path=input_location_path)
        log.info("---Processed File Loaded----")


        # Below is the table schema for Warranty File
        table_schema = {'clam_issue_no': 'string', 'ptyp_part_type': 'string', 'clam_odometer': 'int',
                        'clam_shop_floor': 'string', 'clam_clam_type_dms': 'string', 'clam_repair_dt': 'timestamp',
                        'clam_sratr_date': 'timestamp', 'fyear': 'string', 'ptyp_part_desc': 'string', 'month': 'timestamp',
                        'city': 'string', 'state': 'string', 'vendor_name': 'string', 'clam_vend_cd': 'string',
                        'mile_desc': 'string', 'shop_flr_cd': 'string', 'defect_resp': 'string',
                        'clm2_casual_part': 'string', 'clm2_part_no': 'string', 'region': 'string',
                        'causal_part_name': 'string',
                        'plant': 'string', 'fcok_month': 'timestamp', 'fcok_year': 'string', 'clam_fcok_date': 'timestamp',
                        'clam_process_dt': 'timestamp', 'model_dsec': 'string', 'modl_service_code': 'string',
                        'vdtl_fuel': 'string', 'vdtl_tm_type': 'string', 'vdtl_engi_prefix': 'string',
                        'vdtl_pmod_desc': 'string', 'clam_credit_amt': 'float', 'days_used': 'float',
                        'base_model_code': 'string', 'platform': 'string', 'engine_type': 'string',
                        'glue_last_updated': 'timestamp', 'uid': 'string'}
        log.info("---Schema Extracted----")

        con = wr.redshift.connect(redshift_connection_string)
        log.info("---Redshift Connected----")

        wr.s3.delete_objects(stage_file_path)
        log.info("stage file folder cleaning done")

        turncate_query = f'TRUNCATE TABLE {redshift_schema}.{redshift_table};'
        with con.cursor() as cursor:
            cursor.execute(turncate_query)
            log.info(f'Truncated the TABLE {redshift_schema}.{redshift_table}')
        wr.redshift.copy(df=df, path=stage_file_path, table=redshift_table, schema=redshift_schema, con=con,
                         use_threads=True, mode='append', dtype=table_schema)

        log.info("--- Data moved to Redshift----")

    except Exception as e:
        log.info(f"Error Occurred:{e}")
        raise e
