"""
Note:
    1. As per the discussion with Mahendra San , output file of this job is not changed to hive style partition
    due to business reason as everytime all the files are getting processed
    Jira - https://marutide.atlassian.net/browse/M1-97
    2. Refactoring only includes paramterizing the input and hive style partition on the archiving path
"""

from tqdm import tqdm
tqdm.pandas()
from datetime import date,datetime,timedelta
import DCPArchivefiles,DCPHinkaiFunctions
import awswrangler as wr
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
                                      "part_name_path",
                                      "obs_key_path",
                                      "input_location_path",
                                      "output_path",
                                      "archive_destination"
                                  ])

        part_name_path = args['part_name_path']
        obs_key_path = args['obs_key_path']
        input_location_path = args['input_location_path']
        output_path = args['output_path']
        archive_destination = args['archive_destination']

        log.info("--- File Reading Started----")

        df = DCPHinkaiFunctions.get_hinkai_data(input_location_path)
        log.info("--- File Reading/Merging Complete----")

        part_name_dict, part_name_split_list, part_number_name_mapping = DCPHinkaiFunctions.get_part_name_data(
            part_name_path)
        df[['part_name', 'pn_pred_score', 'part_number']] = df.progress_apply(
            lambda x: DCPHinkaiFunctions.get_part_name(x['all'], part_name_dict, part_name_split_list,
                                                       part_number_name_mapping), axis=1, result_type='expand')
        obs_key_data = DCPHinkaiFunctions.get_obs_data(obs_key_path)
        df[['observation_type', 'category']] = df.progress_apply(
            lambda x: DCPHinkaiFunctions.get_obs(x['all'], obs_key_data), axis=1, result_type='expand')
        df = df[['issue_type', 'model_code', 'hinkai_type', 'prototype_stage', 'title', 'content_explanation',
                 'root_couse', 'c_m', 'defect_resp', 'part_name', 'pn_pred_score', 'part_number',
                 'observation_type',
                 'category']]
        current_time = datetime.now() + timedelta(hours=5, minutes=30)  # current time Indian format
        df['glue_last_updated'] = current_time.strftime('%Y-%m-%d %H:%M:%S')
        df['base_model_code'] = df['model_code'].str[0:3]

        log.info("--- Code Pre-processing Complete----")
        wr.s3.to_csv(df=df, path=output_path, index=False)
        log.info(f"Processed File saved to S3 path : {output_path}")
        value = 'Processed'

        # AWS Proserve: Archival Code - Commented this for testing purpose
        # today = date.today()
        # year = today.strftime("%Y")
        # month = today.strftime("%m")
        # day = today.strftime("%d")
        # archive_destination = f'{archive_destination}/year={year}/month={year}/day={year}'

        # log.info("--- Archiving started----")
        # DCPArchivefiles.move_s3_data(input_location_path, archive_destination, value, suffix=True)
        # log.info("--- Files Archived----")

    except Exception as e:
        log.info(f"The Error={e}")
        raise e

