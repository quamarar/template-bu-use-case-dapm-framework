"""
Note: No code changes are done , only constant path values are put as a glue parameter
"""
import sys
import subprocess

subprocess.check_call([sys.executable, '-m', 'pip', 'install', '--upgrade', 'pip'])

# added on 17-11-2022
subprocess.check_call([sys.executable, '-m', 'pip', 'install', 'importlib-metadata==4.13.0'])
subprocess.check_call([sys.executable, '-m', 'pip', 'install', 'emoji==1.7.0'])
# changed version from 1.10.2 to 1.11.0 on 17-02-2023
subprocess.check_call([sys.executable, '-m', 'pip', 'install', 'torch==1.11.0'])
subprocess.check_call([sys.executable, '-m', 'pip', 'install', 'pyarrow==9.0.0'])

subprocess.check_call([sys.executable, '-m', 'pip', 'install', 'awscli==1.22.80'])
subprocess.check_call([sys.executable, '-m', 'pip', 'install', 'awswrangler==2.16.1'])
subprocess.check_call([sys.executable, '-m', 'pip', 'install', 'boto3==1.21.25'])
subprocess.check_call([sys.executable, '-m', 'pip', 'install', 'gensim==4.0.1'])
subprocess.check_call([sys.executable, '-m', 'pip', 'install', 'nltk==3.4.4'])
subprocess.check_call([sys.executable, '-m', 'pip', 'install', 's3fs==2021.4.0'])
subprocess.check_call([sys.executable, '-m', 'pip', 'install', 'spacy==3.4.0'])

subprocess.check_call([sys.executable, '-m', 'spacy', 'download', 'en_core_web_lg'])
subprocess.check_call([sys.executable, '-m', 'pip', 'install', 'sumy==0.10.0'])
subprocess.check_call([sys.executable, '-m', 'pip', 'install', 'transformers==4.18.0'])
subprocess.check_call([sys.executable, '-m', 'pip', 'install', 'deepspeed'])
subprocess.check_call([sys.executable, '-m', 'pip', 'install', 'sentencepiece'])
subprocess.check_call([sys.executable, '-m', 'pip', 'install', 'git+http://github.com/LIAAD/yake'])

import pandas as pd
import numpy as np
import warnings, traceback

warnings.filterwarnings("ignore")
import awswrangler as wr
import s3fs, os, boto3, awscli
import re, string, json, regex, time, functools, pickle, math
import emoji, spacy
from tqdm import tqdm

tqdm.pandas()
from io import BytesIO

from zipfile import ZipFile
from awscli.clidriver import create_clidriver

import nltk
from nltk.corpus import words
from nltk.metrics.distance import jaccard_distance
from nltk.util import ngrams
from nltk.stem import PorterStemmer

nltk.download('words')
nltk.download('stopwords')
from nltk.corpus import stopwords
from gensim.utils import simple_preprocess
from gensim.parsing.preprocessing import strip_tags

from sumy.parsers.plaintext import PlaintextParser
from sumy.nlp.tokenizers import Tokenizer
from sumy.summarizers.text_rank import TextRankSummarizer
from collections import Counter
from transformers import AutoTokenizer, AutoModel
from torch.nn import functional
import sys
from awsglue.utils import getResolvedOptions


args = getResolvedOptions(sys.argv,
                                  [
                                      "param_folder_name",
                                      "param_bucket",
                                      "param_uri",
                                      "param_part_names_list",
                                      "param_part_names_replace_dict",
                                      "param_part_name_category_dict",
                                       "param_part_name_type_category_mapper",
                                      "param_model_names_list",
                                      "param_model_names_replace_dict",
                                      "param_vehicle_cat_dict",
                                      "param_oem_dict",
                                      "param_category_dict",
                                      "param_feed_dict",
                                      "param_category_dict_trans",
                                      "param_feed_dict_trans",
                                      "param_staging_dest",
                                      "param_archive_dest",
                                      "param_staging_dest",
                                      "param_archive_dest"
                                  ])

param_folder_name = args['param_folder_name']
param_bucket = args['param_bucket']
param_uri = args['param_uri']
param_part_names_list = args['param_part_names_list']
param_part_names_replace_dict = args['param_part_names_replace_dict']
param_part_name_category_dict = args['param_part_name_category_dict']
param_part_name_type_category_mapper = args['param_part_name_type_category_mapper']
param_model_names_list = args['param_model_names_list']
param_model_names_replace_dict = args['param_model_names_replace_dict']
param_vehicle_cat_dict = args['param_vehicle_cat_dict']
param_oem_dict=args['param_oem_dict']
param_category_dict=args['param_category_dict']
param_feed_dict=args['param_feed_dict']
param_category_dict_trans=args['param_category_dict_trans']
param_feed_dict_trans=args['param_feed_dict_trans']
param_staging_dest=args['param_staging_dest']
param_archive_dest=args['param_archive_dest']
param_staging_dest=args['param_staging_dest']
param_archive_dest=args['param_archive_dest']


conn = boto3.client('s3')
bucket = param_bucket
uri = param_uri
fs = s3fs.S3FileSystem(anon=False)
driver = create_clidriver()

CORRECT_SPELLINGS = tuple(words.words())
STOP_WORDS = set(stopwords.words('english'))
NLP = spacy.load('en_core_web_lg')

import yake

language = "en"
max_ngram_size = 1
deduplication_thresold = 0.9
deduplication_algo = 'seqm'
numOfKeywords = 20

custom_kw_extractor = yake.KeywordExtractor(lan=language, \
                                            n=max_ngram_size, \
                                            dedupLim=deduplication_thresold, \
                                            dedupFunc=deduplication_algo, \
                                            top=numOfKeywords, \
                                            features=None
                                            )

cols = ['language', 'snType', 'message', 'snCreatedTime', 'workflowProperties', 'permalink']

path_dict = {
    'folder_name': param_folder_name,
    'bucket': param_bucket,

    'part_names_list': param_part_names_list,
    'part_names_replace_dict': param_part_names_replace_dict,
    'part_name_category_dict': param_part_name_category_dict,
    'part_name_type_category_mapper': param_part_name_type_category_mapper,

    'model_names_list': param_model_names_list,
    'model_names_replace_dict': param_model_names_replace_dict,
    'vehicle_cat_dict': param_vehicle_cat_dict,
    'oem_dict': param_oem_dict,

    'category_dict': param_category_dict,
    'feed_dict': param_feed_dict,
    'category_dict_trans': param_category_dict_trans,
    'feed_dict_trans': param_feed_dict_trans,

    'staging_dest': param_staging_dest,
    'archive_dest': param_archive_dest
}

valid_sn_type = ['TWITTER', 'LINKEDIN', 'DAILYMOTION', 'FACEBOOK', 'YOUTUBE', 'FLICKR', 'FOURSQUARE', \
                 'SLIDESHARE', 'TUMBLR', 'WORDPRESS', 'INSTAGRAM', 'SINAWEIBO', 'RENREN', 'TENCENTWEIBO', \
                 'GOOGLE', 'VK', 'APPLEBUSINESSCHAT', 'RSSFEED', 'SURVEYMONKEY', 'BAZAARVOICE', 'CLARABRIDGE', \
                 'SHIFT', 'PINTEREST', 'JIVE', 'ZIMBRA', 'WECHAT', 'LITHIUM', 'PLUCK', 'LINE', 'XING', \
                 'SOCIALSAFE', 'YAHOO', 'TV', 'PRINT', 'RADIO', 'INTRANET', 'SNAPCHAT', 'SHUTTERSTOCK', \
                 'GETTY', 'FLASHSTOCK', 'VIDMOB', 'TMOBILE', 'MEDIAPLEX', 'KAKAOSTORY', 'SLACK', 'KAKAOTALK', \
                 'VIBER', 'YELP', 'NEXTDOOR', 'REDDIT', 'WHATSAPPBUSINESS', 'WHATSAPP', 'TRUSTPILOT', 'TIKTOK']

rename_cols_dict = {'location': 'location',
                    'additionalInformation': 'additional_information',
                    'snType': 'sn_type',
                    'message': 'message',
                    'snCreatedTime': 'sn_created_time',
                    'textEntities': 'text_entities',
                    'workflowProperties': 'workflow_properties',
                    'permalink': 'permalink',
                    'model name': 'model_name',
                    'part name': 'part_name',
                    'sentiment': 'sentiment',
                    'isSpam': 'is_spam',
                    'isProfane': 'is_profane',
                    'oem': 'oem',
                    'fuel type': 'fuel_type',
                    'vehicle category': 'vehicle_category',
                    'influencerScore': 'influencer_score'
                    }


def get_oem(row):
    """
    Method to handle oem and folder names
    """
    if pd.isna(row['real_oem']):
        return row['folder_name']
    else:
        return row['real_oem']


# Other Utility Fns
def get_zipfile_names(bucket, folder_name, conn):
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
    return [f['Key'] for f in contents if f['Key'].endswith('.zip')]


class FeatureExtractor(object):

    def __init__(self, path_dict):

        self.model_names = pickle.load(fs.open(path_dict['model_names_list'], 'rb'))
        self.model_names_replace = pickle.load(fs.open(path_dict['model_names_replace_dict'], 'rb'))
        self.vehicle_category = pickle.load(fs.open(path_dict['vehicle_cat_dict'], 'rb'))
        self.oem_names_replace = pickle.load(fs.open(path_dict['oem_dict'], 'rb'))
        self.part_names = pickle.load(fs.open(path_dict['part_names_list'], 'rb'))
        self.part_names_replace = pickle.load(fs.open(path_dict['part_names_replace_dict'], 'rb'))
        self.part_names_category = pickle.load(fs.open(path_dict['part_name_category_dict'], 'rb'))
        self.fuel_names = ['petrol', 'diesel', 'cng', 'lpg']

        self.model_pattern = regex.compile(r"\b\L<name>\b", name=self.model_names)
        self.part_pattern = regex.compile(r"\b\L<name>\b", name=self.part_names)
        self.fuel_pattern = regex.compile(r"\b\L<name>\b", name=self.fuel_names)

    def extract_model_names(self, processed_text):
        """
        This method extracts informations related to model name
        Inputs:
            processed_text: str
                social media text

        Returns:
        A tuple of 3 elements
            model_name: str
                the first matching model name
            vehicle_category: str
                the category of the "model_name"
            oem_name: str
                the manufacturer of the "model_name"
        """
        try:
            model = self.model_pattern.search(processed_text).group()
            if model in self.model_names_replace:
                model_name = self.model_names_replace[model]
            else:
                model_name = model
            vehicle_category = self.vehicle_category[model_name]
            oem_name = self.oem_names_replace[model_name]
            return (model_name, vehicle_category, oem_name)
        except:
            return (None, None, None)

    def extract_part_names(self, processed_text):
        """
        This method extracts informations related to part name

        Inputs:
            processed_text: str
                social media text

        Returns:
        A tuple of 2 elements
            part_name: str
                the first matching part name
            part_name_type: str
                the type of the "part_name"
        """
        try:
            part_name = self.part_pattern.search(processed_text).group()
            if part_name in self.part_names_replace:
                part_name_type = self.part_names_replace[part_name]
            else:
                part_name_type = part_name
            part_name_category = self.part_names_category[part_name_type]
            return (part_name, part_name_type, part_name_category)
        except:
            return (None, None, None)

    def extract_fuel_type(self, processed_text):
        """
        This method extracts informations related to fuel type

        Inputs:
            processed_text: str
                social media text

        Returns:
           The matched fuel type
        """
        try:
            return self.fuel_pattern.search(processed_text).group()
        except:
            return None

    def filter_mentions_hashtags(self, raw_text):
        """
        Method to find count of hashtags and mentions

        Input:
            raw_text: str
                social media text

        Returns:
            A boolean value. Returns True if number of both mentions and
            hashtags are less than or equal to 4, else returns False
        """
        mentions = len(re.findall('(?<![\w])@\w+', str(raw_text)))
        hashtags = len(re.findall('(?<![\w])#\w+', str(raw_text)))
        return mentions <= 4 and hashtags <= 4

    def extract_keys(self, workflow_properties, key):
        """
        Method to extract "key" from a dictionary "workflow_properties"

        Inputs:
            workflow_properties: dict
                a dictionary object
            key: str
                the key that we want to extract from the given dictionary
        """
        try:
            if key in workflow_properties.keys():
                return workflow_properties[key]
            else:
                return None
        except:
            return None


class TextProcessor(object):

    def __init__(self, stop_word=True):
        self.stop_word = stop_word
        self.CLEANR = re.compile('<.*?>')

    def preprocess_text(self, raw_text):
        """
        Method to clean the raw sentences

        Inputs:
            tweet: str
                raw social media text
            stop_word: boolean
                flag to remove stop_words from sentences


        Returns:
            tweet: str
                cleaned social media text
        """
        # remove html tags
        raw_text = re.sub(self.CLEANR, ', str(raw_text))

        # Remove @ sign, but keep the text
        raw_text = re.sub("(?<![\w])@(?=\w+)", "", raw_text)

        # Remove http links
        raw_text = re.sub(r"(http?\://|https?\://|www)\S+", " ", raw_text)

        # Remove Emojis
        raw_text = '.join(c for c in raw_text if c not in emoji.UNICODE_EMOJI)

        # Remove # sign, but keep the text
        raw_text = raw_text.replace("#", "").replace("_", " ")

        raw_text = " ".join(w for w in nltk.wordpunct_tokenize(raw_text) if w.isalnum())
        if self.stop_word:
            raw_text = " ".join([i.lower() for i in list(set(raw_text.split())) if i not in STOP_WORDS and len(i) > 2])
        return raw_text


def create_dataframe(file, conn, text_processor, feature_extractor, cols, folder):
    """
    Method to read a raw zip file and load it as pandas DataFrame.
    The process includes intermediate steps like extracting only relevant columns,
    cleaning noisy rows, extraction of some new attributes, etc.

    Once the process is complete, the raw zip files are moved to
    Archive folder.

    Inputs:
        file: str
            name of the zip file
        conn: obj
            connection object for S3
        text_processor: TextProcessor object
            a object from TextProcessor class
        feature_extractor: FeatureExtractor object
            a object from FeatureExtractor class
        cols: list
            the column names that we want to extract
        folder: str
            the folder name where the file is present

    Returns:
        A pandas dataframe

    """
    # 0--> get the zipfile
    zip_obj = conn.get_object(Bucket=bucket, Key=file)
    buffer = BytesIO(zip_obj.get('Body').read())
    z = ZipFile(buffer)
    df = pd.DataFrame(columns=cols)

    # 1--> iterate files inside the zipfile
    for filename in z.namelist():
        with z.open(filename) as f:
            tmp = pd.read_json(f, lines=True)
            try:
                tmp = tmp[cols]
            except:
                tmp = tmp[cols[:-1]]
                tmp[cols[-1]] = np.nan

            df = pd.concat([df, tmp], ignore_index=True)

    # 2--> Extracting the "en" data
    df = df[df['language'] == 'en']

    # 3--> Selecting rows only with valid "snType"
    df = df[df['snType'].str.upper().isin(valid_sn_type)].reset_index(drop=True)

    # 4--> Processing "message"
    df['processed_text'] = df['message'].apply(text_processor.preprocess_text)

    # 5--> Extract model name, vehicle category, and oem
    df[['model_name', 'vehicle_category', 'real_oem']] = df['processed_text'] \
        .apply(feature_extractor.extract_model_names) \
        .to_list()
    # 6--> Extract part name, and part name type
    df[['part_name', 'part_name_type', 'part_name_category']] = df['processed_text'] \
        .apply(feature_extractor.extract_part_names) \
        .to_list()
    # 7--> Extract Fuel Type
    df['fuel_type'] = df['processed_text'].apply(feature_extractor.extract_fuel_type)

    # 8--> Extract messageCategory, isSpam, sentiment, and isProfane
    df['sentiment'] = df['workflowProperties'].apply(lambda x: feature_extractor.extract_keys(x, 'sentiment'))
    df['sentiment'] = df['sentiment'].apply(lambda x: int(x) if str(x) in ["-2", "-1", "0", "1", "2",
                                                                           "-2.0", "-1.0", "0.0", "1.0", "2.0"]
    else 0
                                            )
    df['isSpam'] = df['workflowProperties'].apply(lambda x: feature_extractor.extract_keys(x, 'isSpam'))
    df['isProfane'] = df['workflowProperties'].apply(lambda x: feature_extractor.extract_keys(x, 'isProfane'))
    df['message_category'] = df['workflowProperties'].apply(
        lambda x: feature_extractor.extract_keys(x, 'messageCategory'))

    # 9--> Fix OEM
    df['folder_name'] = folder
    df['oem'] = df[['real_oem', 'folder_name']].apply(get_oem, axis=1)

    # 10--> Add a filter flag
    df['filter_flag'] = df['message'].apply(feature_extractor.filter_mentions_hashtags)

    # 11--> Drop "language", "workflowProperties"
    df.drop(columns=['language', 'workflowProperties'], inplace=True)

    # 12--> Renaming Columns
    df.rename(columns=rename_cols_dict, inplace=True)

    return df


class Predictor(object):

    def __init__(self, ckeys,
                 all_keywords,
                 result_mapper_dict,
                 trans_dict,
                 part_name_type_category_mapper,
                 stop_words,
                 correct_spellings,
                 obj,
                 tokenizer,
                 model,
                 custom_kw_extractor,
                 weight_dict,
                 ac=True
                 ):
        self.ckeys = ckeys
        self.all_keywords = all_keywords
        self.result_mapper_dict = result_mapper_dict
        self.trans_dict = trans_dict
        self.stop_words = stop_words
        self.correct_spellings = correct_spellings
        self.obj = obj
        self.part_name_type_category_mapper = part_name_type_category_mapper
        self.tokenizer = tokenizer
        self.model = model
        self.custom_kw_extractor = custom_kw_extractor
        self.weight_dict = weight_dict
        self.ac = ac

    def make_prediction(self, row):
        """
        Method to obtain prediction for a query point

        Inputs:
            x: str
                social media text
        Returns:
            label: str
                the predicted class label
            score: float
                the score for the prediction
        """
        # predictions are done only if these filter conditions are satisfied
        if pd.isna(row.model_name) or pd.isna(row.part_name) or (not row.filter_flag):
            return (None, None)

        if row.sentiment < 0:

            # get the prediction vector from heuristic model
            v1 = self.get_heuristic_scores(self.yake_extractor(row['processed_text']))

            # get the prediciton vector from transformer model
            v2 = self.get_transformer_scores(self.yake_extractor(row['processed_text']))

            # this function is used for both "category" and "feedback" task.
            # this if:else conditions is used just for that
            if self.obj == 'Category':
                try:
                    mask = self.part_name_type_category_mapper[row.part_name_category]
                except:
                    mask = None
            elif self.obj == 'Feedback':
                mask = None

            return self.combine_prediction(v1, v2, mask)

        else:
            return (None, None)

    def get_heuristic_scores(self, INP):
        """
        Method to find scores against all the classes

        Inputs:
            INP: str
                the social media text, query point
            ac: boolean
                if correction required or not (invokes the method "autocorrect_word" if true)

        Returns:
            sim_vector: list
                a vector of length equal to the number of classes. Each point in the list
                represents the similarity score against a class
        """
        inp = str(INP)
        inp = inp.split()

        # weight for maxsim
        w1 = 0.9
        # weight for avgsim
        w2 = 0.1

        autinp = []

        for x in inp:
            autinp.append(self.autocorrect_word(x))

        # initialize temp variables
        maxsim = [-1.0, -1.0]
        keyword_pos = -1
        sim_vector = []

        # find score for each keylist
        for x in self.ckeys:

            # compare "x" (2d list) with given input "inp", and find a score
            temp1 = self.score(x, inp)

            # find score with autocorrect "ON"
            if self.ac:
                temp2 = self.score(x, autinp)

                # compare "temp1" and "temp2", select the one with higher scores
                temp = []
                if temp1[0] > temp2[0]:
                    temp = temp1
                elif temp1[0] == temp2[0]:
                    if temp1[1] > temp2[1]:
                        temp = temp1
                    else:
                        temp = temp2
                else:
                    temp = temp2
            else:
                temp = temp1

            sim_vector.append(temp[0])

        # "sim_vec" contains the sim score against each of the classes in CKEYS
        return sim_vector

    @functools.lru_cache(maxsize=None)
    def autocorrect_word(self, word):
        """
        Method to apply autocorrection to mispelled words

        Inputs:
            word: str
                the word under concern
        Returns:
            word: str
                word after autocorrection
        """
        if not self.ac or (word in self.correct_spellings):
            return word
        try:
            # correct_spellings len = 236736
            temp = [(jaccard_distance(set(ngrams(word, 2)), set(ngrams(w, 2))), w) for w in self.correct_spellings if
                    w[0] == word[0]]
            for x in sorted(temp, key=lambda val: val[0])[:10]:
                if x[1] in self.all_keywords:
                    return x[1]
            return word
        except:
            return word

    def score(self, keyword_list, sentence):
        """
        Method to find the the score for a class against the social media text

        Inputs:
            keyword_list: list (2D)
                a 2D list return by the breakit function
            sentence: str
                social media text, query point

        Returns:
            (maxs, avgsim): tuple
                Returns the similary score for a class vs query point
        """
        maxs = 0
        avgsim = 0

        # o1 = [['fitment'], ['operation', 'hard']]
        for x in keyword_list:
            csim = 0
            num_in_x = 0

            # x = ['operation', 'hard']
            for y in x:
                if len(y) <= 2 or y in self.stop_words: continue
                m = 0
                num_in_x += 1

                # o2 = "Even Hyundai has withdrawn the manual on the Turbo-GDI on the i20."
                for z in sentence:
                    if len(z) <= 2 or z in self.stop_words: continue

                    f = 0
                    tokens = [self.__getToken(y), self.__getToken(z)]

                    if not tokens[0][0].is_oov and not tokens[1][0].is_oov:
                        f = tokens[0].similarity(tokens[1])
                        if f < 0.35:
                            f = 0
                    # m = the highest score between "y" and "z"
                    m = max(m, f)

                csim += m

            if num_in_x == 0: continue

            # the similarity score for "x"
            csim = csim * 1.0 / num_in_x

            avgsim += csim
            maxs = max(maxs, csim)

        # the avg similarity score among "all x"
        avgsim = avgsim / len(keyword_list)
        return maxs, avgsim

    def get_transformer_scores(self, sentence):
        """
        Method to find scores against all the classes using Transformer model

        Inputs:
            sentecne: str
                the social media text, query point

        Returns:
            sim_vector: list
                a vector of length equal to the number of classes. Each point in the list
                represents the similarity score against a class
        """

        if type(self.trans_dict['keywords'][0]) == list:
            labels = [",".join(i) for i in self.trans_dict['keywords']]
        if len(sentence.split()) > 500:
            sentence = text_summry(sentence)

        inputs = self.tokenizer.batch_encode_plus([sentence] + labels, return_tensors='pt', padding=True)
        input_ids = inputs['input_ids']
        attention_mask = inputs['attention_mask']
        output = self.model(input_ids, attention_mask=attention_mask, )[0]

        sentence_rep = output[:1].mean(dim=1)
        label_reps = output[1:].mean(dim=1)
        sim_vector = functional.cosine_similarity(sentence_rep, label_reps)
        return sim_vector.tolist()

    def combine_prediction(self, v1, v2, mask=None):

        """
        Method to combined the vectors from heuristic and transformer model,
        and obtain one final predictions

        Inputs:
            v1: list
                prediction vector by heuristic model
            v2: list
                prediction vector by transformer model
            mask: list, default=None
                list to subcategorize class predictions (only used for Category predictions)

        Returns:
            label: str
                the predicted class label
            score: float
                the score for the prediction

        """
        if self.obj == 'Category':
            v3 = (np.array(v1) / self.weight_dict['v1_weight_cat']) + (np.array(v2) / self.weight_dict['v2_weight_cat'])
        elif self.obj == 'Feedback':
            v3 = (np.array(v1) / self.weight_dict['v1_weight_feed']) + (
                        np.array(v2) / self.weight_dict['v2_weight_feed'])

        if mask:
            mask_arr = np.ones(v3.size, dtype=bool)
            mask_arr[mask] = False
            v3[mask_arr] = 0
            idx = np.argmax(v3)
        else:
            idx = np.argmax(v3)

        return (self.result_mapper_dict[idx], v3[idx])

    @functools.lru_cache(maxsize=None)
    def __getToken(self, word):
        """
        Get embedding for a word (Spacy module is used here)

        Inputs:
            word: str
                the word for which we want embedding

        Returns:
            word: str
                tokenized word
        """
        return NLP(word)

    def text_summry(self, text):
        """
        Transformers can deal with text only with len <= 500.
        This method is used to summarize sentences with len > 500 into required size

        Inputs:
            text: str

        Returns:
            text_summary:
                summarized text of the input reduced into the length of the text
                input accepted by the transformer model
        """
        # Creating text parser using tokenization
        parser = PlaintextParser.from_string(text, Tokenizer("english"))
        # Summarize using sumy TextRank
        summarizer = TextRankSummarizer()
        summary = summarizer(parser.document, 2)
        text_summary = ""
        for sentence in summary:
            text_summary += str(sentence)
        return text_summary

    def yake_extractor(self, cleaned_text):
        """
        Method to extract the most relevant/repreesentative keywords in a text

        Inputs:
            cleaned_text: str
                social media text (we are doing this on top of processed_text)
        Returns:
            a string created by joining the most important words returned by YAKE
        """
        keywords = self.custom_kw_extractor.extract_keywords(cleaned_text)
        return ' '.join([word[0] for word in keywords])


class LoadVariables(object):

    def __init__(self, path_dict, obj):

        self.path_dict = path_dict
        self.obj = obj

    def breakit(self, ckeys):
        """
        Method to prepare the class keywords into required format

        Inputs:
            CKEYS: list (1D)
                list of keywords for a given class

        Returns:
            B: list (2D)
            eg ["seat belt", "ignition"] -> [["seat", "belt"], "ignition"]

        """
        B = []
        for U in ckeys:
            B += [U.split()]
        return B

    def get_global_variables_fn(self, items_dict):

        """
        Method to prepare the variables required for predictions
        by the heuristic model

        Inputs:
            items_dict: dict
                a dictionary of the form {class_name: list_of_keywords}

        Returns:
            ckeys: list (3D)
                list of observation keys for all the classes. For each class,
                the keywords are a 2D list
            all_keywords: list
                the vocab for the classification task
            result_mapper_dict: list
                the list to map the result to class names

        """
        # prepare 3D of keys for prediction
        ckeys = [self.breakit(tags) for tags in items_dict['values']]

        # all the valid keywords
        all_keywords = set()
        for x in ckeys:
            for y in x:
                for z in y:
                    all_keywords.add(z)

        # list to map predictions
        result_mapper_dict = items_dict['key_names']
        return ckeys, tuple(all_keywords), result_mapper_dict

    def load_variables(self):
        """
        Get method to load variables required for a classification task
        """
        if self.obj == 'Category':
            class_dict = pickle.load(fs.open(path_dict['category_dict'], 'rb'))
            sub_class_dict = pickle.load(fs.open(path_dict['part_name_type_category_mapper'], 'rb'))
            class_dict_trans = pickle.load(fs.open(path_dict['category_dict_trans'], 'rb'))
            ckeys, all_keywords, result_mapper_dict = self.get_global_variables_fn(class_dict)
            return ckeys, all_keywords, result_mapper_dict, class_dict_trans, sub_class_dict

        elif self.obj == 'Feedback':
            class_dict = pickle.load(fs.open(path_dict['feed_dict'], 'rb'))
            class_dict_trans = pickle.load(fs.open(path_dict['feed_dict_trans'], 'rb'))
            ckeys, all_keywords, result_mapper_dict = self.get_global_variables_fn(class_dict)
            return ckeys, all_keywords, result_mapper_dict, class_dict_trans, None


CKEYS_C, ALL_KEYWORDS_C, RESULT_MAPPER_DICT_C, \
    CATEGORY_DICT_TRANS, SUBCLASS_DICT = LoadVariables(path_dict, 'Category').load_variables()
CKEYS_F, ALL_KEYWORDS_F, RESULT_MAPPER_DICT_F, \
    FEEDBACK_DICT_TRANS, _ = LoadVariables(path_dict, 'Feedback').load_variables()

tokenizer = AutoTokenizer.from_pretrained('sentence-transformers/paraphrase-MiniLM-L6-v2')
model = AutoModel.from_pretrained('sentence-transformers/paraphrase-MiniLM-L6-v2')

feature_extractor = FeatureExtractor(path_dict)
text_processor = TextProcessor()

weight_dict = {
    'v1_weight_cat': 0.362,
    'v2_weight_cat': 0.594,
    'v1_weight_feed': 0.00038,
    'v2_weight_feed': 0.019
}

category_predictor = Predictor(CKEYS_C, ALL_KEYWORDS_C, RESULT_MAPPER_DICT_C,
                               CATEGORY_DICT_TRANS, SUBCLASS_DICT, STOP_WORDS,
                               CORRECT_SPELLINGS, 'Category', tokenizer, model,
                               custom_kw_extractor, weight_dict
                               )
feedback_predictor = Predictor(CKEYS_F, ALL_KEYWORDS_F, RESULT_MAPPER_DICT_F,
                               FEEDBACK_DICT_TRANS, None, STOP_WORDS,
                               CORRECT_SPELLINGS, 'Feedback', tokenizer, model,
                               custom_kw_extractor, weight_dict
                               )


def folder_etl(folder, conn, path_dict, cols):
    print(f"{folder} : Processing Started...")
    folder_name = path_dict['folder_name'] + folder.lower()
    file_names = get_zipfile_names(path_dict['bucket'], folder_name, conn)

    if folder == 'MSIL':
        folder = 'Maruti'

    total_file = len(file_names)

    for idx, file in enumerate(file_names):
        try:
            print(f'Processing File : {idx + 1}/{total_file} - {file.split("/")[-1]}')
            start = time.time()

            # 1--> Load the DF
            print('Step 1 --> Creating staging DF')
            df = create_dataframe(file, conn, text_processor, feature_extractor, cols, folder)

            # 2--> Make predictions
            print('Step 2 --> Running Predtions (Category)')
            df["category"], df["category_score"] = zip(
                *df[['model_name', 'part_name', 'filter_flag', 'processed_text', 'part_name_category', 'sentiment']] \
                .apply(category_predictor.make_prediction, axis=1)
                )
            print('Step 2 --> Running Predtions (Feedback)')
            df["feedback"], df["feedback_score"] = zip(
                *df[['model_name', 'part_name', 'filter_flag', 'processed_text', 'part_name_category', 'sentiment']] \
                .apply(feedback_predictor.make_prediction, axis=1)
                )

            # 3--> Save to locations
            # a) Save to "processed"
            print('Step 3 --> Saving Results (saving to staging)')
            df.to_parquet(f"{path_dict['staging_dest']}{folder}/{file.split('/')[-1][:-4]}.parquet.gzip",
                          compression='gzip')

            # b) Archive the zipfile
            print('Step 3 --> Saving Results (archiving raw file)')
            src = f"s3://{path_dict['bucket']}/{file}"
            dest = f"{path_dict['archive_dest']}{folder}/"
            # driver.main(f's3  mv  {src}  {dest}'.split('  '))

            print(f'File Processed in {time.time() - start} seconds')

        except Exception as e:
            print('Exception occured:', traceback.format_exc())

    print(f"{folder} : Processing Complete!")


for folder in ['Kia', 'Hyundai', 'MSIL', 'Tata', 'Mahindra']:
    try:
        folder_etl(folder, conn, path_dict, cols)
    except:
        print('Exception occured:', traceback.format_exc())