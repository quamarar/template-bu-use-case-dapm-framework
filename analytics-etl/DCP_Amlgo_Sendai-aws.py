"""
Note:
    1. As per the discussion with Mahendra San , output file of this job is not changed to hive style partition
    due to business reason as everytime all the files are getting processed
    Jira - https://marutide.atlassian.net/browse/M1-97
    2. Refactoring only includes paramterizing the input and hive style partition on the archiving path
"""
# subprocess.check_call([sys.executable, '-m', 'pip', 'install', 'xlrd'])
# subprocess.check_call([sys.executable, '-m', 'pip', 'install', 'fuzzywuzzy'])
# subprocess.check_call([sys.executable, '-m', 'pip', 'install', 'python-Levenshtein'])

import sys, subprocess, nltk, re, DCPArchivefiles, DCPSendaiFunctions
from datetime import date, datetime, timedelta
import awswrangler as wr
import pandas as pd
from fuzzywuzzy import fuzz
from tqdm import tqdm

import logging
import sys
from awsglue.utils import getResolvedOptions

log = logging.getLogger(__name__)
logging.basicConfig(format='%(asctime)s :: %(message)s', level=logging.INFO)
logging.info("info")

tqdm.pandas()


def get_session(date):
    if pd.isna(date):
        return ''
    else:
        return pd.to_datetime(pd.Series([date])).dt.to_period('Q-MAR').dt.qyear.apply(
            lambda x: str(x - 1) + "-" + str(x))



if __name__ == '__main__':
    try:

        args = getResolvedOptions(sys.argv,
                                  [
                                      "input_location_path",
                                      "output_path",
                                      "part_name_path",
                                      "archive_destination",
                                      "obs_key_path"
                                  ])

        input_location_path = args['input_location_path']
        output_path = args['output_path']
        part_name_path = args['part_name_path']
        archive_destination = args['archive_destination']
        obs_key_path = args['obs_key_path']



        log.info("--- File Reading Started----")
        obj_list = wr.s3.list_objects(input_location_path)
        obj_list = [obj for obj in obj_list if obj.split(".")[-1] in ('csv', 'xlsx', 'xls')]
        log.info(obj_list)
        # try:
        for path in obj_list:
            df = pd.read_csv(path)  # ,engine='openpyxl')
            df.columns = [DCPSendaiFunctions.get_clean_name(i) for i in df.columns]

        log.info("--- File Reading/Merging Complete and now starting preprocessing----")

        df['defect_resp'] = df['responsible_person_department'].apply(lambda x: DCPSendaiFunctions.get_dfect_resp(x))
        part_name_dict, part_name_split_list, part_number_name_mapping = DCPSendaiFunctions.get_part_name_data(
            part_name_path)
        df[['part_name', 'pn_pred_score', 'part_number']] = df.progress_apply(
            lambda x: DCPSendaiFunctions.get_part_name(x['defect'], part_name_dict, part_name_split_list,
                                                       part_number_name_mapping), axis=1, result_type='expand')
        obs_key_data = DCPSendaiFunctions.get_obs_data(obs_key_path)
        df[['observation_type', 'category']] = df.progress_apply(
            lambda x: DCPSendaiFunctions.get_obs(x['defect'], obs_key_data), axis=1, result_type='expand')
        final_columns = ['s_no', 'model', 'defect_vin_no', 'defect', 'category_of_defect',
                         'defect_reporting_date', 'reporting_date', 'type_of_defect',
                         'responsibility_design_prod_vendor_kd',
                         'class_of_root_cause_operator_env_method_control_mat', 'responsible_person_department',
                         'root_cause',
                         'countermeasure', 'observation_ananlysis', 'part_name', 'pn_pred_score', 'part_number',
                         'observation_type', 'category', 'defect_resp']
        df = df[final_columns]
        df['session'] = pd.to_datetime(df['reporting_date']).dt.to_period('Q-MAR').dt.qyear.apply(
            lambda x: str(x - 1) + "-" + str(x))
        current_time = datetime.now() + timedelta(hours=5, minutes=30)  # current time Indian format
        df['glue_last_updated'] = current_time.strftime('%Y-%m-%d %H:%M:%S')
        df['pmonth_year'] = pd.to_datetime(df['reporting_date']).dt.strftime('%b-%Y')
        df['base_model_code'] = df['model'].str[0:3]

        log.info("--- Code Pre-processing Complete----")
        wr.s3.to_csv(df, path=output_path, index=False)
        log.info("--- Processed File saved to S3----")
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
        log.info(e)
        raise e


