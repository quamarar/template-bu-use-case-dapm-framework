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
        

        df = wr.s3.read_parquet(path=path)

        print("---Data Loaded----")

        table_schema = {'month_year': 'timestamp',
                        'model_code': 'string', 'serial_id': 'int', 'session': 'string',
                        'count': 'float', 'glue_last_updated': 'timestamp', 'base_model_code': 'string'}
        print("---Schema Extracted----")

        con = wr.redshift.connect("dcp_redshift_connect_Acc_7673")
        print("---Redshift Connected----")

        stage_file_path = 's3://dcpanalyticsdev/Defect_Correlation_Prediction/Tempdata/production'
        wr.s3.delete_objects(stage_file_path)
        print("stage file folder cleaning done")

        truncate_query = f'TRUNCATE TABLE {redshift_schema}.{redshift_table}'
        with con.cursor() as cursor:
            cursor.execute(truncate_query)
            print(f'TRUNCATED THE TABLE {redshift_schema}.{redshift_table}')
        wr.redshift.copy(df=df, path=stage_file_path, table=redshift_table, schema=redshift_schema, con=con,
                         use_threads=True, mode='append', dtype=table_schema)

        print("--- Data moved to Redshift----")
        con.close()


        print("--- code Running End----")
    except Exception as e:
        print(f"Error Occurred:{e}")
        raise

