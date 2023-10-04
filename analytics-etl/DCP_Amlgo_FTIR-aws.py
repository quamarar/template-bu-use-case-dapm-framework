"""
Note:
    1. As per the discussion with Mahendra San , output file of this job is not changed to hive style partition
    due to business reason as everytime all the files are getting processed
    Jira - https://marutide.atlassian.net/browse/M1-97
    2. Refactoring only includes paramterizing the input and hive style partition on the archiving path
"""


import awswrangler as wr, re, boto3, math, DCPArchivefiles
import pandas as pd
import numpy as np
from tqdm import tqdm
from collections import Counter
from datetime import date, datetime, timedelta
import logging
import sys
from awsglue.utils import getResolvedOptions

log = logging.getLogger(__name__)
logging.basicConfig(format='%(asctime)s :: %(message)s', level=logging.INFO)
logging.info("info")

tqdm.pandas()



def get_split_key(keys):
    keys = re.sub('/ +', '/', keys.lower())
    keys = keys.split("/")
    keys = [[i] if len(i.split()) == 1 else i.split() for i in keys]
    keys = [x for xs in keys for x in xs]
    return keys


def counter_cosine_similarity(c1, c2):
    c1 = Counter(c1)
    c2 = Counter(c2)
    terms = set(c1).union(c2)
    dotprod = sum(c1.get(k, 0) * c2.get(k, 0) for k in terms)
    magA = math.sqrt(sum(c1.get(k, 0) ** 2 for k in terms))
    magB = math.sqrt(sum(c2.get(k, 0) ** 2 for k in terms))
    return dotprod / (magA * magB)


def get_obs(sent: str, obs_key_data: dict):
    sent = sent.lower().split(" ")
    output_dict = {'obs': [], 'score': []}
    for key, obs in zip(obs_key_data['keywords'], obs_key_data['observation_type']):
        output_dict['score'].append(counter_cosine_similarity(sent, key))
        output_dict['obs'].append(obs)
    output = pd.DataFrame(output_dict).sort_values(by='score', ascending=False)
    return output.iloc[0]['obs'], round(output.iloc[0]['score'] * 100, 2)


def get_session(date):
    try:
        if pd.isna(date):
            return ''
        else:
            return pd.to_datetime(pd.Series([date])).dt.to_period('Q-MAR').dt.qyear.apply(
                lambda x: str(x - 1) + "-" + str(x))
    except:
        return ''


def getNum(x):
    ret = 0
    for i in str(x):
        if i >= '0' and i <= '9':
            ret = ret * 10 + (int(i))
    return ret


def get_dfect_resp(text):
    if pd.isna(text):
        return 'PRODUCTION'
    text = text.lower().strip()
    if text.startswith('en') or text.startswith('yo'):
        return 'DESIGN'
    elif text.startswith('Quality') or text.startswith('qa'):
        return 'VENDOR'
    else:
        return 'PRODUCTION'


if __name__ == '__main__':
    try:

        args = getResolvedOptions(sys.argv,
                                  [
                                      "input_location_path",
                                      "output_path",
                                      "ftir_observation_file_path",
                                      "archive_destination",
                                      "partnamepath"
                                  ])

        input_location_path = args['input_location_path']
        output_path = args['output_path']
        ftir_observation_file_path = args['ftir_observation_file_path']
        archive_destination = args['archive_destination']
        partnamepath = args['partnamepath']


        reqcols = ['Segmentation', 'Product Model Code', 'Subject (English)', 'Date of Incident', 'Causal Parts Name (English)',
                   'Days Used', 'Mileage / Using Time', 'Causal Parts No.', 'Department of Action Judgement', 'SBPR No.']
        obs_data = pd.read_excel(ftir_observation_file_path,engine='openpyxl', names=['keywords', 'observation_type'])


        partname = pd.read_csv(partnamepath)
        log.info(' reading part_name_list_mapping_final_v2 done')


        log.info("--- File Reading Started----")
        obj_list = wr.s3.list_objects(input_location_path)
        obj_list = [obj for obj in obj_list if obj.split(".")[-1] in ('xlsx', 'csv')]
        log.info(obj_list)

        # try:
        df = pd.DataFrame(data=None, columns=reqcols)
        for path in obj_list:
            ftir_files = pd.read_excel(path, engine='openpyxl', usecols=reqcols)
            log.info(f'excel read has done for the path {path}')
            df = pd.concat([df, ftir_files])



        log.info("--- File Reading/Merging Complete----")


        log.info("--- Code Pre-processing STarted----")
        df = df[
            ['Segmentation', 'Product Model Code', 'Subject (English)', 'Date of Incident', 'Causal Parts Name (English)',
             'Days Used', 'Mileage / Using Time', 'Causal Parts No.', 'Department of Action Judgement', 'SBPR No.']]
        df.columns = ['segmentation', 'product_model_code', 'subject_english', 'date_of_incident',
                      'causal_parts_name_english', 'days_used', 'mileage_using_time', 'causal_parts_no', 'Defect_resp',
                      'sbpr_no']
        partmap = dict(zip(partname['part_number'], partname['part_name']))
        df['causal_parts_name_english'] = df['causal_parts_no'].apply(lambda x: partmap.get(x, ''))
        obs_data['keywords'] = obs_data['keywords'].apply(lambda x: get_split_key(x))
        obs_key_data = obs_data.to_dict(orient='list')
        df['subject_english'] = df['subject_english'].fillna(value='')
        df['subject_english'] = df['subject_english'].progress_apply(lambda x: get_obs(x, obs_key_data)[0])
        df['session'] = pd.to_datetime(df['date_of_incident']).dt.to_period('Q-MAR').dt.qyear.apply(
            lambda x: str(x - 1) + "-" + str(x))
        current_time = datetime.now() + timedelta(hours=5, minutes=30)  # current time Indian format
        df['glue_last_updated'] = current_time.strftime('%Y-%m-%d %H:%M:%S')
        df['pmonth_year'] = pd.to_datetime(df['date_of_incident']).dt.strftime('%b-%Y')
        df['index'] = list(range(len(df)))
        df['uid'] = df['index'].apply(lambda x: str(x)) + '-' + df['glue_last_updated'].apply(lambda x: str(x))
        df.drop('index', axis=1, inplace=True)
        df['mileage'] = df['mileage_using_time'].apply(getNum)
        df['days'] = df['days_used'].apply(getNum)
        df['defect_response'] = df['Defect_resp'].apply(get_dfect_resp)
        df['base_model_code'] = df['product_model_code'].str[0:3]
        df = df[df['product_model_code'].notnull()]
        model_code_del = ['Y', 'Y1W', 'Y2', 'Y50', 'Y50M1', 'Y51', 'Y8', 'Y6K14', 'Y6K14BBC', 'Y6K14BBD', 'YB1', 'YC%',
                          'YCG', 'YE2', 'YL*', 'YLA51C2C', 'YM1', 'YM125', 'YN4', 'YN411', 'YN4B2', 'YN4B2C2B', 'YN4B2C2C',
                          'YN4B2C2D', 'YN4D2C2B', 'YN4D2C2C', 'YN4D2C2D', 'YR4BED', 'YR4CSH', 'YR4BEP', 'YS', 'Y1E19',
                          'Y3A', 'Y3AM', 'Y3AM1', 'Y3AS1', 'Y5A', 'Y5AF9', 'Y5AF9BAA', 'Y5AF9BAB', 'Y5AF9BAC', 'YP7R0',
                          'YXXXX']
        base_code_add = ['Y1W', 'Y50', 'Y51', 'Y6K', 'YB1', 'YCG', 'YE2', 'YLA', 'YM1', 'YN4', 'YR4', 'Y1E', 'Y5A', 'Y3A',
                         'YXX', 'YP7']
        df = df[~df['product_model_code'].isin(model_code_del)]
        df = df[df['product_model_code'].astype(str).str.startswith('Y')]
        df = df[~df['base_model_code'].isin(base_code_add)]

        log.info(f"Code Pre-processing Complete and file is getting populated at path :{output_path}")
        wr.s3.to_csv(df, path=output_path, index=False)
        log.info("--- Processed File saved to S3----")
        value = 'Processed'

        # AWS Proserve : Archival code - This code is commented for testing purpose
        # today = date.today()
        # year = today.strftime("%Y")
        # month = today.strftime("%m")
        # day = today.strftime("%d")
        # archive_destination = f'{archive_destination}/year={year}/month={year}/day={year}'

        log.info("--- Archiving started---------")
        DCPArchivefiles.move_s3_data(input_location_path, archive_destination, value, suffix=True)
        log.info("--- Files Archived----")
    except Exception as e:
        log.info(f"The Error={e}")
        raise e

