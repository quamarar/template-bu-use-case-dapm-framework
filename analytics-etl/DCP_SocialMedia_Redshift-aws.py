"""
Note: No code changes are done , only constant path values are put as a glue parameter
"""
import sys
import subprocess
import traceback

# implement pip as a subprocess:
# pip         21.3.1
subprocess.check_call([sys.executable, '-m', 'pip', 'install', '--upgrade', 'pip'])
subprocess.check_call([sys.executable, '-m', 'pip', 'install', 'awscli==1.22.80'])
subprocess.check_call([sys.executable, '-m', 'pip', 'install', 'awswrangler==2.16.1'])
subprocess.check_call([sys.executable, '-m', 'pip', 'install', 'boto3==1.21.25'])
subprocess.check_call([sys.executable, '-m', 'pip', 'install', 'numpy==1.18.5'])
subprocess.check_call([sys.executable, '-m', 'pip', 'install', 'pandas==1.1.5'])
subprocess.check_call([sys.executable, '-m', 'pip', 'install', 'pyarrow==9.0.0'])

import pandas as pd
import numpy as np
import awswrangler as wr
import warnings, time, boto3, awscli

warnings.filterwarnings("ignore")
from awscli.clidriver import create_clidriver
from uuid import uuid4
from datetime import date, datetime, timedelta
import sys
from awsglue.utils import getResolvedOptions

conn = boto3.client('s3')
driver = create_clidriver()


args = getResolvedOptions(sys.argv,
                                  [
                                      "param_bucket",
                                      "param_staging_dest",
                                      "param_processed_dest",
                                      "param_redshift_dest",
                                      "redshift_connection_string",
                                      "redshift_schema"
                                  ])

param_bucket = args['param_bucket']
param_staging_dest = args['param_staging_dest']
param_processed_dest = args['param_processed_dest']
param_redshift_dest = args['param_redshift_dest']
redshift_connection_string = args['redshift_connection_string']
redshift_schema = args['redshift_schema']

folders = ['Hyundai', 'Kia', 'Tata', 'Mahindra', 'MSIL']

valid_sn_type = ['TWITTER', 'LINKEDIN', 'DAILYMOTION', 'FACEBOOK', 'YOUTUBE', 'FLICKR', 'FOURSQUARE', \
                 'SLIDESHARE', 'TUMBLR', 'WORDPRESS', 'INSTAGRAM', 'SINAWEIBO', 'RENREN', 'TENCENTWEIBO', \
                 'GOOGLE', 'VK', 'APPLEBUSINESSCHAT', 'RSSFEED', 'SURVEYMONKEY', 'BAZAARVOICE', 'CLARABRIDGE', \
                 'SHIFT', 'PINTEREST', 'JIVE', 'ZIMBRA', 'WECHAT', 'LITHIUM', 'PLUCK', 'LINE', 'XING', \
                 'SOCIALSAFE', 'YAHOO', 'TV', 'PRINT', 'RADIO', 'INTRANET', 'SNAPCHAT', 'SHUTTERSTOCK', \
                 'GETTY', 'FLASHSTOCK', 'VIDMOB', 'TMOBILE', 'MEDIAPLEX', 'KAKAOSTORY', 'SLACK', 'KAKAOTALK', \
                 'VIBER', 'YELP', 'NEXTDOOR', 'REDDIT', 'WHATSAPPBUSINESS', 'WHATSAPP', 'TRUSTPILOT', 'TIKTOK']
path_dict = {
    'bucket': param_bucket,

    'staging_dest': param_staging_dest,
    'processed_dest': param_processed_dest,
    'redshift_dest': param_redshift_dest

    #     'staging_dest'  : 'Defect Correlation_Prediction_QA/AMLGO Files/ProcessedFiles/TulroseDeori/temp_v5_aug10/temp_staging/',
    #     'processed_dest': 's3://analytics-msil-dev/Defect Correlation_Prediction_QA/AMLGO Files/ProcessedFiles/TulroseDeori/temp_v5_aug10/temp_processed/',
    #     'redshift_dest' : 's3://analytics-msil-dev/Defect Correlation_Prediction_QA/AMLGO Files/ProcessedFiles/TulroseDeori/temp_v5_aug10/temp_redshift/'
}


# Other Utility Fns
def get_parquet_names(bucket, folder_name, conn):
    """
    Get the list of zipfile names in "folder_name"

    Inputs:
        bucket: str
            the name of the s3 bucket
        folder_name: str
            the folder name we want to look at
        conn: obj
            connection object for s3

    Returns
        A list of file names in the "folder_name"
    """
    contents = conn.list_objects(Bucket=bucket, Prefix=folder_name)['Contents']
    return [f['Key'] for f in contents if f['Key'].endswith('.parquet.gzip')]


table_schema = {
    'id': 'string',
    'sn_type': 'string',
    'message': 'string',
    'sn_created_time': 'timestamp',
    'permalink': 'string',
    'processed_text': 'string',
    'model_name': 'string',
    'vehicle_category': 'string',
    'part_name': 'string',
    'part_name_type': 'string',
    'part_name_category': 'string',
    'fuel_type': 'string',
    'sentiment': 'integer',
    'is_spam': 'string',
    'is_profane': 'string',
    'message_category': 'string',
    'oem': 'string',
    'category': 'string',
    'category_score': 'float',
    'feedback': 'string',
    'feedback_score': 'float',
    'folder_name': 'string',
    'real_oem': 'string',
    'filter_flag': 'integer',
    'glue_last_updated': 'timestamp',
    'sn_created_time_month': 'timestamp',
    'session': 'string'

}


def extract_month_year(x):
    return pd.Timestamp(x).replace(day=1)


def trim_text(text, size):
    trm = str(text)[:size]
    if len(trm.encode(encoding='unicode-escape')) < size:
        return trm
    else:
        trm = trm.encode(encoding='unicode-escape')[:size]
        try:
            return trm.decode('unicode-escape')
        except:
            trm = trm.decode('ASCII')
            return trm.rsplit(' ', 1)[0].encode('ASCII').decode('unicode-escape')


def load_redshift_files(folder, conn, path_dict, redshift_con, table_name, current_time):
    print(f"{folder} : Processing Started...")
    if folder == 'MSIL':
        folder = 'Maruti'
    folder_name = path_dict['staging_dest'] + folder
    print(path_dict['bucket']+folder_name)
    file_names = get_parquet_names(path_dict['bucket'], folder_name, conn)

    total_file = len(file_names)

    for idx, file in enumerate(file_names):
        try:
            print(f'Processing File : {idx + 1}/{total_file} - {file.split("/")[-1]}')
            start = time.time()

            # 1--> Load the DF
            print('Step 1 --> Loading staged DF')
            df = wr.s3.read_parquet(f"s3://{path_dict['bucket']}/{file}")

            # 2) Add to Redshift
            print('Step 2 --> Saving Results (Redshift)')
            wr.s3.delete_objects(path_dict['redshift_dest'])
            print("---stage file folder cleaning done---")

            # add unique identifier
            df['id'] = df.apply(lambda x: datetime.now().strftime('%y%m%d-%H%M%S%f') + str(uuid4()), axis=1)

            # add glue job runtime timestamp
            df['glue_last_updated'] = current_time

            # add the month column
            df['sn_created_time_month'] = df['sn_created_time'].apply(extract_month_year).astype('str')

            # add session
            df['session'] = pd.to_datetime(df['sn_created_time']).dt.to_period('Q-MAR').dt.qyear.apply(
                lambda x: str(x - 1) + "-" + str(x))

            for k, v in table_schema.items():
                if v not in ['date', 'timestamp', 'integer']:
                    df[k] = df[k].astype(v)

            # trim text according to table schema
            df['message'] = df['message'].apply(lambda x: trim_text(x, 4000))
            df['processed_text'] = df['processed_text'].apply(lambda x: trim_text(x, 4000))
            df['permalink'] = df['permalink'].apply(lambda x: str(x)[:255])

            wr.redshift.copy(df=df, path=path_dict['redshift_dest'],
                             table=table_name,
                             schema="amalgo_zone",
                             con=redshift_con,
                             use_threads=True,
                             dtype=table_schema,
                             varchar_lengths={'message': 5000, 'processed_text': 5000},
                             mode='append'
                             )
            print("---data moved to Redshift---")

            # 3) Move the CSV
            print('Step 3 --> Move file from staging to processed')
            src = f"s3://{path_dict['bucket']}/{file}"
            dest = f"{path_dict['processed_dest']}{folder}/"
            driver.main(f's3  mv  {src}  {dest}'.split('  '))

            print(f'File Processed in {time.time() - start} seconds')

        except Exception as e:
            print('Exception occured in "load_redshift_files()":', traceback.format_exc())

    print(f"{folder} : Processing Complete!")


TABLE_NAME = 'sentiment_data_cleaned'

query_remove_duplicates = f"""
    DELETE FROM {redshift_schema}.{TABLE_NAME} WHERE id IN (
        SELECT a.id FROM (
            SELECT * from {redshift_schema}.{TABLE_NAME}
            WHERE message IS NOT NULL
        ) a
        INNER JOIN (
            SELECT COUNT(message), MAX(message) as message 
            FROM {redshift_schema}.{TABLE_NAME}  
            WHERE message IS NOT NULL 
            GROUP BY message HAVING COUNT(message) > 1
        ) a1
        ON a.message = a1.message
        LEFT JOIN (
            SELECT COUNT(message), MAX(id) as id 
            FROM {redshift_schema}.{TABLE_NAME}  
            WHERE message IS NOT NULL 
            GROUP BY message HAVING COUNT(message) > 1
        ) b
        ON a.id = b.id
        WHERE a.message IS NOT NULL AND b.id is null
    )
    """

query_update_messages_neutral = f"""
    UPDATE {redshift_schema}.{TABLE_NAME}
    SET message = NULL, processed_text = NULL
    WHERE MONTHS_BETWEEN(CURRENT_DATE, sn_created_time) <= 12 AND sentiment = 0
    """

query_update_messages_year_old = f"""
    UPDATE {redshift_schema}.{TABLE_NAME}
    SET message = NULL, processed_text = NULL
    WHERE MONTHS_BETWEEN(CURRENT_DATE, sn_created_time) > 12
    """

query_row_count = f"""
    SELECT COUNT(*) FROM {redshift_schema}.{TABLE_NAME}
    """

redshift_con = wr.redshift.connect(redshift_connection_string)
redshift_con.autocommit = True
print("Redshift Connected")

CURRENT_TIME = datetime.now() + timedelta(hours=5, minutes=30)  # current time Indian format
CURRENT_TIME = CURRENT_TIME.strftime('%Y-%m-%d %H:%M:%S')

# run task
for folder in ['Maruti', 'Mahindra', 'Tata', 'Kia', 'Hyundai']:

    try:
        print('1 --> Loading Files to Redshift')
        t1 = time.time()
        load_redshift_files(folder, conn, path_dict, redshift_con, TABLE_NAME, CURRENT_TIME)
        print(f'1 --> Loading Files to Redshift Completed in ({round(time.time() - t1, 0)} secs)')

    except:
        print('Exception occured in Loading Files to Redshift:', traceback.format_exc())

try:
    t2 = time.time()
    print("2 Remove Duplicates --> Query Execution Starts")

    pre = wr.redshift.read_sql_query(
        sql=query_row_count,
        con=redshift_con
    )
    print(f'*** Before row count --> {pre["count"].iloc[0]}')

    with redshift_con.cursor() as cursor:
        cursor.execute(query_remove_duplicates)

    post = wr.redshift.read_sql_query(
        sql=query_row_count,
        con=redshift_con
    )
    print(f'*** After row count --> {post["count"].iloc[0]}')

    print(f"2 Remove Duplicates --> Query Execution Completed in ({round(time.time() - t2, 0)} secs)")

    t3 = time.time()
    print("3 Update Messages --> Query Execution Starts")
    with redshift_con.cursor() as cursor:
        cursor.execute(query_update_messages_neutral)
        print(f"*** Current Year Neutral ***")
        cursor.execute(query_update_messages_year_old)
        print(f"*** Last 1 Year ***")
        print(f"3 Update Messages --> Query Execution Completed in ({round(time.time() - t3, 0)} secs)")

except:
    print('Exception occured in SQL Queries:', traceback.format_exc())

redshift_con.close()
