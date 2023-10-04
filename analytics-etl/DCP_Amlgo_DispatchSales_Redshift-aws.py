import awswrangler as wr
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
        # day='21'
        path = f"{input_location_path}/year={year}/month={month}/day={day}"


        df = wr.s3.read_parquet(path=path)
        log.info("---Data Loaded----")


        table_schema = {'month_year': 'timestamp',
                        'model_code': 'string', 'model_code_full': 'string', 'serial_id': 'int',
                        'count': 'int', 'session': 'string', 'City': 'string', 'State': 'string',
                        'glue_last_updated': 'timestamp', 'base_model_code': 'string'}
        log.info("---Schema Extracted----")

        con = wr.redshift.connect(redshift_connection_string)
        log.info("---Redshift Connected----")

        wr.s3.delete_objects(stage_file_path)
        log.info("stage file folder cleaning done")


        turncate_query = f'truncate table {redshift_schema}.{redshift_table};'
        
        
        with con.cursor() as cursor:
            cursor.execute(turncate_query)
            log.info(f"Truncated the table {redshift_schema}.{redshift_table}")
        wr.redshift.copy(df=df, path=stage_file_path, table=redshift_table, schema=redshift_schema, con=con,
                         use_threads=True, mode='append', dtype=table_schema)

        log.info("--- Data moved to Redshift----")
        con.close()

    except Exception as e:
        log.info(f"The Error={e}")
        raise e

