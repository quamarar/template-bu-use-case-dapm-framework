print("--- code Running Started----")
import awswrangler as wr
import pandas as pd
import os, sys
import boto3
from datetime import date,datetime,timedelta
from io import BytesIO
print("--- Libraries Loaded----")

#s3://dcpanalyticsdev/Defect_Correlation_Prediction/ProcessedData/Line/Line.parquet
#Change below to update locations for Glue Job.
bucket='dcpanalyticsdev'
data_folder='Defect_Correlation_Prediction'

today = date.today()
year = today.strftime("%Y")
month = today.strftime("%m")
day = today.strftime("%d")
# day='21'

# file_name = f'{data_folder}/ProcessedData/Line/Line.parquet'
# output_path = f"s3://{bucket}/{data_folder}/ProcessedData/Line/Line.parquet"
path = f"s3://{bucket}/{data_folder}/ProcessedData/Line/year={year}/month={month}/day={day}"


#output_path = f"s3://{bucket}/{data_folder}/Masterdatasetup/OutputInformation/Line/Line.csv"





try:
    # s3 = boto3.client('s3') 
    # obj = s3.get_object(Bucket= bucket, Key=file_name)
    # buffer = BytesIO(obj['Body'].read())
    # df = pd.read_parquet(buffer)
    #df = pd.read_parquet(obj['Body'])
    df = wr.s3.read_parquet(path=path)
except Exception as e:
    print(f"Error:{e}")
else:
    print("---Data Loaded----")

    table_schema = {'model_code': 'string',
                     'part_number': 'string', 'part_name': 'string',
                     'vendor_code': 'string', 'vendor_name': 'string',
                     'observation': 'string', 'defect_category': 'string',
                     'rep_defect': 'string', 'causes_category': 'string',
                     'plant': 'string', 'pmonth_year': 'timestamp',
                     'session': 'string', 'month': 'string','glue_last_updated': 'timestamp','defect_resp':'string',
                     'wc_dept_code':'string', 'wc_name':'string', 'wc_rec_no':'string','base_model_code':'string',
                     'date': 'timestamp','control_no': 'string','last_update_on': 'timestamp','line_status': 'string','location': 'string'
                    }
#     table_schema = {
#         'sr_no': 'int',
#  'vendor_code': 'string',
#  'vendor_name': 'string',
#  'dept': 'string',
#  'vendor_category': 'string',
#  'pilot_line_implementation': 'string',
#  'assessment_score': 'float',
#  'assessment_status': 'string',
#  'session': 'string'}
    
    print("---Schema Extracted----")
    
    con = wr.redshift.connect("dcp_redshift_connect")
    print("---Redshift Connected----")
    
    stage_file_path = 's3://dcpanalyticsdev/Defect_Correlation_Prediction/StageFiles/TempFiles/Line/'
    wr.s3.delete_objects(stage_file_path)
    print("stage file folder cleaning done")
    
    try:
        turncate_query = 'TRUNCATE TABLE dcp_zone.line_data;'
        with con.cursor() as cursor:
            cursor.execute(turncate_query)
        wr.redshift.copy(df=df,path=stage_file_path,table="line_data",schema="dcp_zone",con=con,use_threads=True,mode='append',dtype=table_schema)
    except Exception as e:
        print(f"Error Occurred:{e}")
        raise
    else:
        print("--- Data moved to Redshift----")
        con.close()
    
    # print("--- Archiving started----")
    # obj = output_path.split("/")[-1]
    # now = datetime.now()+timedelta(hours=5, minutes=30)
    # suffix =now.strftime('%Y-%m-%d %H:%M:%S')
    # file=obj.split(".")[0]+suffix+"."+obj.split(".")[1]
    # output_path = f"s3://{bucket}/{data_folder}/ProcessedData/Line/{file}"
    # wr.s3.to_parquet(df=df, path=output_path,index=False)
    # wr.s3.delete_objects(path) 
    # print("--- Files Archived----")
    
    print("--- code Running End----")