import argparse
import logging
import os
import sys
import time
import boto3
import pandas as pd
from sklearn.preprocessing import MinMaxScaler
from utils.ddb_helper_functions import upload_file,read_json_from_s3,update_ssm_store
from utils.dynamodb_util import TrainInputDataModel, TrainingMetaDataModel, Timelaps
import traceback

#################
# import argparse
# import logging
# import time
# import boto3
# import pandas as pd
# from sklearn.preprocessing import MinMaxScaler
# from model.utils.ddb_helper_functions import  upload_file, read_json_from_s3
# from model.utils.dynamodb_util import TrainInputDataModel, TrainingMetaDataModel, Timelaps
##################
job_name = "training_gatekeeper"
log = logging.getLogger(__name__)
logging.basicConfig(format=' %(job_name)s - %(asctime)s - %(message)s ')
def args_parse():
    """
    :return: arguments
    """
    constants ={}
    # Create the parser
    parser = argparse.ArgumentParser()
    parser.add_argument('--train_metatable_name', type=str, required=True)
    parser.add_argument('--region', type=str, required=True)
    return constants, parser.parse_args()


    
def combine_harmonize_data(meta_table_item, region, file_path=None) -> pd.DataFrame:
    """
     this method is a boilerplate code thus needs to be changed on the basis of business ml use case
    :param file_path:
    :return: DataFrame
    """
    path = "s3://"
    combined_df = pd.read_csv(file_path)
    return combined_df


def ml_preprocessing(combined_dataframe, mapping_id) -> pd.DataFrame:
    """
    this method is a boilerplate code thus needs to be changed on the basis of business ml use case
    :param combined_dataframe: dataset dataframe after combining data from different source
    :param mapping_id: mappingid is comming from mapping json file ,value can be "default","region" etc
    :return: DataFrame
    """
    df = combined_dataframe
    df_column_filter_list = ['part_name', 'base_model', 'engine_type', 'vdtl_tm_type', 'vdtl_fuel', 'platform',
                             'part_probability','mapping',
                             mapping_id]
    df = df[df_column_filter_list]
    df = pd.get_dummies(df, columns=['base_model', 'engine_type', 'vdtl_tm_type', 'vdtl_fuel', 'platform'])
    scaler = MinMaxScaler()
    df[['part_probability']] = scaler.fit_transform(df[['part_probability']])
    return df

def insert_train_job_def_input_table(pk_mappingid, step_job_id, usecase_name,
                                     execution_year, execution_month, execution_day, pkid,
                                     mapping_id, mapping_json_s3_path, algo_execution_status, algo_names,
                                     s3_pk_mappingid_data_input_path, s3_output_bucket_name,
                                     batch_job_definition, batch_job_status_overall, input_data_set, recursive_run=0,
                                     **kwargs) -> int:
    """
    Takes input all columns and saves data in TrainInput DDB Table
    :param pk_mappingid: pk|mapping_id( primary key)
    :param step_job_id: step function execution id
    :param usecase_name: logical name of your solution/usecase
    :param execution_year: yyyy
    :param execution_month: mm
    :param execution_day: dd
    :param pkid: unique string
    :param mapping_id:  mappingid is comming from mapping json file ,value can be "default","region" etc
    :param algo_execution_status: empty list
    :param s3_pk_mappingid_data_input_path: path where data is saved for pk_mappingid for training
    :param s3_output_bucket_name:  value is coming from metatable
    :param batch_job_definition: aws batch job arn
    :param algo_names: list of algorithms for which model will be trained
    :param batch_job_status_overall: default value to be given here as "TO_BE_CREATED"
    **kwargs
    :return: exit code
    """
    print(
        " About to insert into Input DynamoDB table for partition key {} and reursive num runs {}".format(pk_mappingid,
                                                                                                          recursive_run))
    try:
        TrainInputDataModel(pk_mappingid=pk_mappingid,
                            step_job_id=step_job_id,
                            usecase_name=usecase_name,
                            execution_year=execution_year,
                            execution_month=execution_month,
                            execution_day=execution_day,
                            pk=pkid,
                            mapping_id=mapping_id,
                            mapping_json_s3_path=mapping_json_s3_path,
                            input_data_set=input_data_set,
                            algo_execution_status=algo_execution_status,
                            algo_names=algo_names,
                            s3_pk_mappingid_data_input_path=s3_pk_mappingid_data_input_path,
                            s3_output_bucket_name=s3_output_bucket_name,
                            batch_job_definition=batch_job_definition,
                            batch_job_status_overall=batch_job_status_overall,
                            ).save()

    except Exception as error:
        logging.error(error)
        if recursive_run == 3:
            return False
        time.sleep(1)
        insert_train_job_def_input_table(pk_mappingid, step_job_id, usecase_name,
                                         execution_year, execution_month, execution_day, pkid,
                                         mapping_id, mapping_json_s3_path, algo_execution_status, algo_names,
                                         s3_pk_mappingid_data_input_path, s3_output_bucket_name,
                                         batch_job_definition, batch_job_status_overall, input_data_set,
                                         recursive_run + 1)

    return True


if __name__ == "__main__":
    try:
        pre_processing_start_epoch = int(time.time())
        s3_client = boto3.client('s3')

        _,args = args_parse()

        # Meta Table name and Region required to instantiate TrainingMetaDataModel
        TrainingMetaDataModel.setup_model(TrainingMetaDataModel, args.train_metatable_name, args.region)
        meta_item = TrainingMetaDataModel.get("fixedlookupkey")
        TrainInputDataModel.setup_model(TrainInputDataModel, meta_item.train_inputtable_name, meta_item.region)

        # DynamoDB Table Creation
        if not TrainInputDataModel.exists():
            TrainInputDataModel.create_table(read_capacity_units=100, write_capacity_units=100)
            time.sleep(10)

        # Read mapping json from Amazon S3
        mapping_json_constants = read_json_from_s3(meta_item.mapping_json_s3_path, s3_client)
        primaryKey = mapping_json_constants["mapping_json_data"]["primary_key"]
        mapping_id = mapping_json_constants["mapping_json_data"]['Training']["mappingColumn"]
        all_algo_names = mapping_json_constants["mapping_json_data"]["Training"]["all_algorithms"]

        # Input Path for Preprocessing Job - this will require change based on the usecase ( intermediate file path to savr combined dataset)
        # analytical_data_path = "s3://{}/{}".format(meta_item.s3_bucket_name_internal, "analytical_data/sample_data.csv")

        # File path where intermediate files are stores - required for the feature store and dashboard data quality
        # features_dq_input_path = f"s3://{meta_item.s3_bucket_name_internal}/analytical_data/{meta_item.execution_year}/{meta_item.execution_month}/{meta_item.execution_day}"

        ############# Cutsom logic for preprocessing begins here  ############################
        
        combined_df, analytical_data_path = combine_harmonize_data(region=args.region, meta_table_item=meta_item)
        
        logging.info(f"Mapping Id is: {mapping_id}")
        if mapping_id == "default":
            combined_df["mapping"] = 'default'
        else:
            combined_df["mapping"] = combined_df[mapping_id]
        combined_df[mapping_id] = combined_df["mapping"]
            
        ml_dataset = ml_preprocessing(combined_df, mapping_id)

        # "bucket/trigger_eval_summary/year={year}/month={}/day={}/stepjobid={}"
        # s3_output_path = """s3:///framework-msil-poc-apsouth1-shared/preprocessing/year=execution_year
        #                                        /month=execution_month/day=execution_day/stepjobid=step_execution_id/
        #                                        sm_id=step_execution_id/""".format()

        # preprocess_output_path = "s3://{}/{}".format(shared_s3_bucket,"preprocessing")
        # "this will be used in in_path"
        # Bucket for each use case is different - hence the preprocessing sub-folder will never overlap
        preprocess_output_path = "s3://{}/preprocessing/year={}/month={}/day={}/stepjobid={}/smid={}/".format(meta_item.s3_bucket_name_shared,
                                             meta_item.execution_year,
                                             meta_item.execution_month,
                                             meta_item.execution_day,
                                             meta_item.step_job_id,
                                             meta_item.step_job_id)

        ml_dataset["pk"] = ml_dataset[primaryKey]
        ml_dataset["pk"] = ml_dataset["pk"].apply(lambda x : ''.join(letter for letter in x if letter.isalnum()))
        
        ml_dataset.to_parquet(preprocess_output_path, partition_cols=["pk", 'mapping'])
        filtered_data = ml_dataset.drop_duplicates(subset=[primaryKey, mapping_id], keep="last")
        
        log.info("dataframe 1 record : {}".format(filtered_data.head(1)))

        s3_preprocessing_prefix_output_path = (
            "s3://{}/preprocessing/year={}/month={}/day={}/stepjobid={}/smid={}/").format(
            meta_item.s3_bucket_name_shared,
            meta_item.execution_year,
            meta_item.execution_month,
            meta_item.execution_day,
            meta_item.step_job_id,
            meta_item.step_job_id)


        total_num_training_jobs = 0
        
        # for index, row in filtered_data.iloc[:15].iterrows():
        for index, row in filtered_data.iterrows():
            total_num_training_jobs = total_num_training_jobs + 1


            # "smid is same as step function id"


            s3_pk_mappingid_data_input_path = ("s3://{}/preprocessing/year={}/month={}/day={}/stepjobid={}/smid={}/pk={}/mapping={}/").format(
                     meta_item.s3_bucket_name_shared,
                           meta_item.execution_year,
                           meta_item.execution_month,
                           meta_item.execution_day,
                           meta_item.step_job_id,
                           meta_item.step_job_id,
                           row.pk,
                           row[mapping_id])
            log.info("s3_pk_mappingid_data_input_path: {}".format(s3_pk_mappingid_data_input_path))
            #preprocess_output_bucket_name = "s3://{}".format(meta_item.s3_bucket_name_shared)
            aws_batch_job_definition = meta_item.aws_batch_job_definition

            status_input_job = insert_train_job_def_input_table(pk_mappingid=row.pk + "|" + row[mapping_id],
                                                                step_job_id= meta_item.step_job_id,
                                                                usecase_name=meta_item.usecase_name,
                                                                execution_year=meta_item.execution_year,
                                                                execution_month=meta_item.execution_month,
                                                                execution_day=meta_item.execution_day,
                                                                pkid=row.pk,
                                                                mapping_id=row[mapping_id],
                                                                mapping_json_s3_path=meta_item.mapping_json_s3_path,
                                                                algo_execution_status=[],
                                                                algo_names=all_algo_names,
                                                                s3_pk_mappingid_data_input_path=s3_pk_mappingid_data_input_path,
                                                                s3_output_bucket_name=meta_item.s3_bucket_name_shared,
                                                                batch_job_definition=aws_batch_job_definition,
                                                                batch_job_status_overall="TO_BE_CREATED",
                                                                input_data_set=[analytical_data_path]
                                                                )
            if not status_input_job:
                raise Exception("Preprocessing failed!")

        pre_processing_end_epoch = int(time.time())

        ############# Cutsom logic for preprocessing ends here  ############################


        meta_item.preprocessing_timelaps = Timelaps(start_time=pre_processing_start_epoch,
                                                    end_time=pre_processing_end_epoch)
        meta_item.features_dq_input_path = analytical_data_path
        meta_item.algo_names=all_algo_names
        meta_item.preprocessing_total_batch_jobs = total_num_training_jobs
        meta_item.s3_preprocessing_prefix_output_path = s3_preprocessing_prefix_output_path
        meta_item.save()
        print("Training Preprocessing Complete!")
    except Exception as error:
        log.error("Error ->{}".format(error))
        traceback.print_exc()
        log.info("Training Preprocessing Job failed to Complete!")
        sys.exit(1)

    # docker run -v ~/.aws:/root/.aws 731580992380.dkr.ecr.ap-south-1.amazonaws.com/msil-preprocessing:latest  --region ap-south-1 --train_metatable_name trainmetatable
