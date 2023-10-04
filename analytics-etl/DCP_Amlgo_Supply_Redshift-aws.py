
import awswrangler as wr
from datetime import date
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

        path = f"{input_location_path}/year={year}/month={month}/day={day}"
        
        log.info(f"File to be read from the s3 path : {path}")
        df = wr.s3.read_parquet(path=path)

        log.info("---Data Loaded----")

        table_schema = {'part_number': 'string',
                        'root_part_number': 'string', 'count': 'float',
                        'vendor_name': 'string', 'vendor_code': 'string',
                        'month_year': 'timestamp', 'serial_id': 'int', 'glue_last_updated': 'timestamp'}
        log.info("---Schema Extracted----")

        con = wr.redshift.connect(redshift_connection_string)
        log.info("---Redshift Connected----")

        # stage_file_path = 's3://dcpanalyticsdev/Defect_Correlation_Prediction/Tempdata/supply'
        wr.s3.delete_objects(stage_file_path)
        log.info(f"File deleted from stage_file_path: {stage_file_path}")


        turncate_query = f'truncate table {redshift_schema}.{redshift_table};'
        with con.cursor() as cursor:
            cursor.execute(turncate_query)
            log.info(f"trunncated the table with query : {turncate_query}")
        wr.redshift.copy(df=df, path=stage_file_path, table=redshift_table, schema=redshift_schema, con=con,
                         use_threads=True, mode='append', dtype=table_schema)

        log.info("--- Data moved to Redshift----")
        con.close()

    except Exception as e:
        log.info(f"Error Occurred:{e}")
        raise e