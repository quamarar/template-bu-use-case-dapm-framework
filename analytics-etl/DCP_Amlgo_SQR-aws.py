"""
Note:
    1. As per the discussion with Mahendra San , output file of this job is not changed to hive style partition
    due to business reason as everytime all the files are getting processed
    Jira - https://marutide.atlassian.net/browse/M1-97
    2. Refactoring only includes paramterizing the input and hive style partition on the archiving path
"""

import awswrangler as wr,  boto3, DCPArchivefiles, DCPSQRFunctions
import nltk
from datetime import date, datetime, timedelta

import logging
import sys
from awsglue.utils import getResolvedOptions

log = logging.getLogger(__name__)
logging.basicConfig(format='%(asctime)s :: %(message)s', level=logging.INFO)
logging.info("info")


try:
    log.info("Loading nltk library")
    from nltk.corpus import stopwords
    words = set(nltk.corpus.words.words())
    stop_words = stopwords.words('english')
except:
    log.info("Except block of loading nltk library")
    nltk.download('words')
    nltk.download('punkt')
    nltk.download('stopwords')
    from nltk.corpus import stopwords
    words = set(nltk.corpus.words.words())
    stop_words = stopwords.words('english')


if __name__ == '__main__':
    try:

        args = getResolvedOptions(sys.argv,
                                  [
                                      "part_name_path",
                                      "input_location_path",
                                      "output_path",
                                      "archive_destination",
                                      "obs_key_path"
                                  ])

        part_name_path = args['part_name_path']
        input_location_path = args['input_location_path']
        output_path = args['output_path']
        archive_destination = args['archive_destination']
        obs_key_path = args['obs_key_path']

        
        
        log.info("--- File Reading Started----")
        obj_list = wr.s3.list_objects(input_location_path)
        obj_list = [obj for obj in obj_list if obj.split(".")[-1] in ('xlsx', 'csv')]
        log.info(obj_list)

        df = DCPSQRFunctions.get_sqr_data(input_location_path)

        log.info("--- File Reading/Merging Complete----")

        part_name_dict, part_name_split_list, part_number_name_mapping = DCPSQRFunctions.get_part_name_data(part_name_path)
        df['temp_part_name_pred'] = df['all'].progress_apply(
            lambda x: DCPSQRFunctions.get_part_name(x, part_name_dict, part_name_split_list, part_number_name_mapping))
        df['part_name'] = df['temp_part_name_pred'].apply(lambda x: x[0])
        df['pn_pred_score'] = df['temp_part_name_pred'].apply(lambda x: x[1])
        df['part_number'] = df['temp_part_name_pred'].apply(lambda x: x[2])
        obs_key_data = DCPSQRFunctions.get_obs_data(obs_key_path)
        df['temp_obs_cat'] = df['all'].progress_apply(lambda x: DCPSQRFunctions.get_obs(x, obs_key_data))
        df['observation_type'] = df['temp_obs_cat'].apply(lambda x: x[0])
        df['category'] = df['temp_obs_cat'].apply(lambda x: x[1])
        df = df[['issue_type', 'model_code', 'hinkai_type', 'prototype_stage', 'title', 'content_explanation', 'root_couse',
                 'c_m', 'defect_resp', 'part_name', 'pn_pred_score', 'part_number', 'observation_type', 'category']]
        current_time = datetime.now() + timedelta(hours=5, minutes=30)  # current time Indian format
        df['glue_last_updated'] = current_time.strftime('%Y-%m-%d %H:%M:%S')
        df['base_model_code'] = df['model_code'].apply(lambda x: x[:3])

        log.info("--- Code Pre-processing Complete----")
        wr.s3.to_csv(df, path=output_path, index=False)
        # wr.s3.to_parquet(df=df,path=output_path,dataset=True,index=False)
        log.info("--- Processed File saved to S3----")
        value = 'Processed'

        
        # AWS Proserve : Archival code - This code is commented for testing purpose
        # today = date.today()
        # year = today.strftime("%Y")
        # month = today.strftime("%m")
        # day = today.strftime("%d")
        # archive_destination = f'{archive_destination}/year={year}/month={year}/day={year}'
        # 
        # log.info("--- Archiving started----")
        # DCPArchivefiles.move_s3_data(input_location_path, archive_destination, value, suffix=True)
        # log.info("--- Files Archived----")
    except Exception as e:
        log.info(f"The Error={e}")
        raise e