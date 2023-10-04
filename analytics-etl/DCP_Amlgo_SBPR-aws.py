"""
Note:
    1. As per the discussion with Mahendra San , output file of this job is not changed to hive style partition
    due to business reason as everytime all the files are getting processed
    Jira - https://marutide.atlassian.net/browse/M1-97
    2. Refactoring only includes paramterizing the input and hive style partition on the archiving path
"""


# import sys,subprocess, time
# subprocess.check_call([sys.executable, '-m', 'pip', 'install', 'fuzzywuzzy'])
# from fuzzywuzzy import fuzz
import awswrangler as wr, re,boto3,math,DCPArchivefiles, string
from datetime import date,datetime,timedelta
import pandas as pd
import nltk
from tqdm import tqdm
tqdm.pandas()
from collections import Counter

import logging
import sys
from awsglue.utils import getResolvedOptions

log = logging.getLogger(__name__)
logging.basicConfig(format='%(asctime)s :: %(message)s', level=logging.INFO)
logging.info("info")

try:
    log.info("---- importing nltk.corpus----")
    from nltk.corpus import stopwords
    stop_words = stopwords.words('english')
except:
    log.info("---- except block of importing nltk.corpus----")
    nltk.download('punkt')
    nltk.download('stopwords')
    from nltk.corpus import stopwords
    stop_words = stopwords.words('english')
log.info("---- Libraries Loaded----")

def get_split_key(keys):
    keys = re.sub(', +', ',',keys.lower())
    keys = keys.split(",")
    keys = [[i] if len(i.split())==1 else i.split() for i in keys]
    keys = [x for xs in keys for x in xs]
    return keys

def counter_cosine_similarity(c1, c2):
    c1 = Counter(c1)
    c2 = Counter(c2)
    terms = set(c1).union(c2)
    dotprod = sum(c1.get(k, 0) * c2.get(k, 0) for k in terms)
    magA = math.sqrt(sum(c1.get(k, 0)**2 for k in terms))
    magB = math.sqrt(sum(c2.get(k, 0)**2 for k in terms))
    return dotprod / (magA * magB)

def get_obs_data(obs_key_path:str):
    obs_key_data = pd.read_csv(obs_key_path)
    obs_key_data['keywords'] = [get_split_key(i) for i in obs_key_data['keywords'].to_list()]
    obs_key_data = obs_key_data.to_dict(orient='list')
    return obs_key_data

def get_obs(sent:str,obs_key_data:dict):
    get_cat = dict(zip(obs_key_data['observation_type'],obs_key_data['category']))
    output_dict = {'obs':[],'labels':[],'score':[],'category':[]}
    sent = sent.lower().split(" ")
    for key,obs in zip(obs_key_data['keywords'],obs_key_data['observation_type']):
        output_dict['score'].append(counter_cosine_similarity(sent, key))
        output_dict['labels'].append(key)
        output_dict['obs'].append(obs)
        output_dict['category'].append(get_cat[obs])
    output = pd.DataFrame(output_dict).sort_values(by='score',ascending=False)
    return output.iloc[0]['obs'],output.iloc[0]['category']

def get_dfect_resp(text):
    if pd.isna(text):
        return 'PRODUCTION'
    text = text.lower().strip()
    if text.startswith('en') or text.startswith('yo'):
        return 'DESIGN'
    elif text.startswith('qa'):
        return 'VENDOR'
    else:
        return 'PRODUCTION'

def remove_dash(text):
    if pd.isna(text):
        return text
    if len(text)>5:
        if text[5]=='-':
            return text[:5]+text[6:]
    return text


if __name__ == '__main__':
    try:
        args = getResolvedOptions(sys.argv,
                                  [
                                      "input_location_path",
                                      "output_path",
                                      "item_comp_path",
                                      "archive_destination",
                                      "vendor_name_file_path",
                                      "obs_key_path"
                                  ])

        input_location_path = args['input_location_path']
        output_path = args['output_path']
        item_comp_path = args['item_comp_path']
        archive_destination = args['archive_destination']
        vendor_name_file_path = args['vendor_name_file_path']
        obs_key_path = args['obs_key_path']


        reqcols=['Segmentation','Product Model Code (SBPR)','Subject (English)','Incident Overview (English)','Causal Parts No.','Responsible Supplier','Date of Summary','FTIRs in the SBPR','Responsible Dgt']


        log.info("--- File Reading Started----")
        obj_list = wr.s3.list_objects(input_location_path)
        obj_list = [obj for obj in obj_list if obj.split(".")[-1] in ('xlsx','csv')]
        log.info(obj_list)

        df=pd.DataFrame(data=None,columns=reqcols)
        for path in obj_list:
            sbpr_files=pd.read_excel(path,engine='openpyxl', usecols=reqcols)
            log.info(f"File read with the path: {path}")
            df=pd.concat([df,sbpr_files])

        log.info("--- File Reading/Merging Complete----")


        item=pd.read_csv(item_comp_path,delimiter="$",low_memory=False)
        item=item[['ITEM_CODE','ITEM_DESC']]

        df1=pd.merge(df,item,how='left',left_on='Causal Parts No.', right_on='ITEM_CODE')
        df1 = df1.drop_duplicates()

        vd=pd.read_csv(vendor_name_file_path)
        df2=pd.merge(df1,vd,how='left',left_on='Responsible Supplier', right_on='vendor_name')

        df2 = df2.drop_duplicates()
        df2.drop(['ITEM_CODE','vendor_name'],inplace=True,axis=1)
        df2['Date of Summary'] = pd.to_datetime(df2['Date of Summary'],infer_datetime_format=True,errors='coerce')
        df2['Date of Summary'] = df2['Date of Summary'].fillna(method="ffill")
        df2['Product Model Code (SBPR)'] = df2['Product Model Code (SBPR)'].str[:3]
        df2['text'] = df2[['Subject (English)','Incident Overview (English)']].values.tolist()
        df2['text'] = df2['text'].progress_apply(lambda x:" ".join([i for i in x if type(i) == str]))
        obs_key_data = get_obs_data(obs_key_path)
        df2['temp_obs_cat'] = df2['text'].progress_apply(lambda x:get_obs(x,obs_key_data))
        df2['observation_type']  = df2['temp_obs_cat'].apply(lambda x:x[0])
        df2['category']  = df2['temp_obs_cat'].apply(lambda x:x[1])
        df2['defect_resp']=df2['Responsible Dgt'].apply(lambda x:get_dfect_resp(x))
        df2['Causal Parts No.'] = df2['Causal Parts No.'].progress_apply(lambda x:remove_dash(x))
        df_final = df2[['Segmentation', 'Product Model Code (SBPR)', 'Subject (English)',
           'Incident Overview (English)', 'Causal Parts No.',
           'Responsible Supplier', 'Date of Summary', 'FTIRs in the SBPR', 'observation_type', 'category','defect_resp','ITEM_DESC','vendor_code']]
        df_final.columns = ['segmentation', 'product_model_code', 'subject_english','incident_overview_english', 'causal_parts_no',
           'responsible_supplier', 'date_of_summary', 'ftirs_sbpr','observation_type', 'category','defect_resp','causal_parts_name_english','vendor_code']
        df_final['session'] = df_final['date_of_summary'].dt.to_period('Q-MAR').dt.qyear.apply(lambda x: str(x-1) + "-" + str(x))
        current_time = datetime.now()+timedelta(hours=5, minutes=30) # current time Indian format
        df_final['glue_last_updated'] = current_time.strftime('%Y-%m-%d %H:%M:%S')
        df_final['index']=list(range(len(df_final)))
        df_final['uid']=df_final['index'].apply(lambda x:str(x))+'-'+df_final['glue_last_updated'].apply(lambda x:str(x))
        df_final.drop('index', axis=1,inplace=True)

        log.info("--- Code Pre-processing Complete----")
        wr.s3.to_csv(df_final, path=output_path, index=False)
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