import argparse
import logging
import time
import boto3
import sys
import pandas as pd
import traceback
from utils.ddb_helper_functions import upload_file, read_json_from_s3, read_ssm_store, update_ssm_store
from utils.inference_dynamodb_model import InferenceMetaDataModel, InferenceInputDataModel, Timelaps
from sklearn.preprocessing import MinMaxScaler

#################
# import argparse
# import logging
# import time
# import boto3
# import pandas as pd
# from sklearn.preprocessing import MinMaxScaler
# from model.utils.ddb_helper_functions import  upload_file, read_json_from_s3
# from model.utils_inference.inference_dynamodb_model import InferenceMetaDataModel, InferenceInputDataModel, Timelaps
##################
log = logging.getLogger("inferencing_preprocessing")
# logging.basicConfig(format='%(asctime)s - %(filename)s - %(funcName)s %(lineno)d -- %(message)s', level=logging.INFO)
logging.basicConfig(format='%(levelname)-8s [%(filename)s:%(lineno)d] %(message)s',
                    datefmt='%Y-%m-%d:%H:%M',
                    level=logging.DEBUG)


def args_parse():
    """
    :return: constant, arguments
    """

    # Create the parser
    parser = argparse.ArgumentParser()
    parser.add_argument('--inference_metatable_name', type=str, required=True)
    parser.add_argument('--region', type=str, required=True)
    return parser.parse_args()


def combine_harmonize_data(meta_table_item, region, file_path=None) -> pd.DataFrame:
    """
    :param file_path:
    :return: DataFrame
    """
    path = "s3://"
    combined_df = pd.read_csv(path)
    combined_data_destination_path =path
    return combined_df,path


def ml_preprocessing(combined_dataframe, mapping_id) -> pd.DataFrame:
    """
    this method is a boilerplate code thus needs to be changed on the basis of business ml use case
    :param combined_dataframe:
    :param mapping_id:
    :return: DataFrame
    """
    df = combined_dataframe
    df = df[['part_name', 'base_model', 'engine_type', 'vdtl_tm_type',
             'vdtl_fuel', 'platform', 'region']]
    df = pd.get_dummies(df, columns=[
        'base_model', 'engine_type', 'vdtl_tm_type', 'vdtl_fuel', 'platform'])
    # scaler = MinMaxScaler()
    # df[['part_probability']] = scaler.fit_transform(df[['part_probability']])
    return df




def insert_inference_job_def_input_table(pk_mappingid, step_job_id, usecase_name,
                                         execution_year, execution_month, execution_day, pkid,
                                         mapping_id, mapping_json_s3_path, algo_names,
                                         s3_pk_mappingid_data_input_path, s3_output_bucket_name,
                                         batch_job_definition, batch_job_status_overall, input_data_set,
                                         meta_table_item,
                                         s3_pk_mapping_model_prefix_input_path,
                                         recursive_run=0,
                                         **kwargs
                                         ) -> int:
    """
    Takes input all columns and saves data in TrainInput DDB Table
    :param pk_mappingid: pk|mapping_id( primary key)
    :param step_job_id:
    :param usecase_name:
    :param execution_year: yyyy
    :param execution_month: mm
    :param execution_day: dd
    :param pkid: unique string
    :param mapping_id:
    :param algo_execution_status:
    :param s3_pk_mappingid_data_input_path:
    :param s3_output_bucket_name:
    :param batch_job_definition: algo_names:
    :param batch_job_status_overall:
    :param meta_table_item:
    **kwargs
    :return: exit code
    """
    log.info(
        " About to insert into Input DynamoDB table for partition key {} and reursive num runs {},meta_table_item {}".format(
            pk_mappingid,
            recursive_run,
            meta_table_item))
    try:
        InferenceInputDataModel(pk_mappingid=pk_mappingid,
                                inference_step_job_id=step_job_id,
                                training_step_job_id=meta_table_item.training_step_job_id,

                                inference_usecase_name=usecase_name,
                                inference_execution_year=execution_year,
                                inference_execution_month=execution_month,
                                inference_execution_day=execution_day,

                                training_execution_year=meta_table_item.training_execution_year,
                                training_execution_month=meta_table_item.training_execution_month,
                                training_execution_day=meta_table_item.training_execution_day,
                                pk=pkid,
                                mapping_id=mapping_id,
                                mapping_json_s3_inference_path=mapping_json_s3_path,
                                mapping_json_s3_training_path=meta_table_item.mapping_json_s3_training_path,

                                inference_input_data_set=input_data_set,

                                inference_algo_names=algo_names,
                                training_algo_names=meta_table_item.training_algo_names,

                                s3_pk_mappingid_data_input_path=s3_pk_mappingid_data_input_path,
                                s3_pk_mapping_model_prefix_input_path=s3_pk_mapping_model_prefix_input_path,
                                s3_output_bucket_name=s3_output_bucket_name,
                                batch_job_definition=batch_job_definition,
                                batch_job_status_overall=batch_job_status_overall,
                                ).save()

    except Exception as error:
        logging.error(error)
        if recursive_run == 3:
            return False
        time.sleep(1)
        insert_inference_job_def_input_table(pk_mappingid, step_job_id, usecase_name,
                                             execution_year, execution_month, execution_day, pkid,
                                             mapping_id, mapping_json_s3_path, algo_names,
                                             s3_pk_mappingid_data_input_path, s3_output_bucket_name,
                                             batch_job_definition, batch_job_status_overall, input_data_set,
                                             meta_table_item,
                                             s3_pk_mapping_model_prefix_input_path,
                                             recursive_run + 1)

    return True


if __name__ == "__main__":
    try:
        pre_processing_start_epoch = int(time.time())
        s3_client = boto3.client('s3')

        args = args_parse()
        # args = vars(args)
        log.info("Preprocessing Arguments {}".format(args))
        # Meta Table name and Region required to instantiate TrainingMetaDataModel
        InferenceMetaDataModel.setup_model(
            InferenceMetaDataModel, args.inference_metatable_name, args.region)
        meta_item = InferenceMetaDataModel.get("fixedlookupkey")
        InferenceInputDataModel.setup_model(
            InferenceInputDataModel, meta_item.inference_inputtable_name, meta_item.region)


        # DynamoDB Table Creation
        if not InferenceInputDataModel.exists():
            log.info("Creating InferenceInputDataModel...")
            InferenceInputDataModel.create_table(
                read_capacity_units=100, write_capacity_units=100)
            time.sleep(10)
            
        # Read mapping json from Amazon S3
        mapping_json_constants = read_json_from_s3(meta_item.mapping_json_s3_inference_path, s3_client)
        primaryKey = mapping_json_constants["mapping_json_data"]["primary_key"]
        mapping_id = mapping_json_constants["mapping_json_data"]['Inference']["mappingColumn"]

        # Input Path for Preprocessing Job
        # analytical_data_path = "s3://{}/{}".format(
        #     meta_item.s3_bucket_name_internal, "inference_data/inference_data.csv")

        # combined_df = combine_harmonize_data(analytical_data_path)
        # ml_dataset = ml_preprocessing(combined_df)
        
        # combined_df, analytical_data_path = combine_harmonize_data(meta_table_item=meta_item)
        # ml_dataset = ml_preprocessing(combined_df, mapping_id)
        
        combined_df, analytical_data_path = combine_harmonize_data(region=args.region, meta_table_item=meta_item)
        
        logging.info(f"Mapping Id is: {mapping_id}")
        if mapping_id == "default":
            combined_df['mapping'] = 'default'
        else:
            combined_df['mapping'] = combined_df[mapping_id]
        
        combined_df[mapping_id] = combined_df["mapping"]   
        ml_dataset = ml_preprocessing(combined_df, mapping_id)
        

        # "bucket/trigger_eval_summary/year={year}/month={}/day={}/stepjobid={}"
        # s3_output_path = """s3:///framework-msil-poc-apsouth1-shared/preprocessing/year=execution_year
        #                                        /month=execution_month/day=execution_day/stepjobid=step_execution_id/
        #                                        sm_id=step_execution_id/""".format()

        # preprocess_output_path = "s3://{}/{}".format(shared_s3_bucket,"preprocessing")
        # "this will be used in in_path"
        s3_preprocessing_prefix_output_path = "s3://{}/preprocessing/year={}/month={}/day={}/stepjobid={}/smid={}/".format(
            meta_item.s3_bucket_name_shared,
            meta_item.inference_execution_year,
            meta_item.inference_execution_month,
            meta_item.inference_execution_day,
            meta_item.inference_step_job_id,
            meta_item.inference_step_job_id)
        log.info('s3_preprocessing_prefix_output_path  {}'.format(
            s3_preprocessing_prefix_output_path))
        log.info('mapping inference path {}'.format(
            meta_item.mapping_json_s3_inference_path))

        ml_dataset["pk"] = ml_dataset[primaryKey]
        ml_dataset["pk"] = ml_dataset["pk"].apply(lambda x : ''.join(letter for letter in x if letter.isalnum()))

        log.info("ML Dataset record count per pk and mapping")
        log.info(ml_dataset.groupby(["pk", mapping_id]).size())

        ml_dataset.to_parquet(s3_preprocessing_prefix_output_path,
                              partition_cols=["pk", "mapping"])
        try:
            log.info("primaryKey:{},mapping_id:{}".format(primaryKey, mapping_id))
            if mapping_id == "default":
                filtered_data = ml_dataset.drop_duplicates(
                    subset=[primaryKey], keep="last")
            else:
                filtered_data = ml_dataset.drop_duplicates(
                    subset=[primaryKey, mapping_id], keep="last")
        except Exception as error:
            raise Exception(
                "Inference DataSet does not contain mapping or primarykey column as mentioned in Mapping Json")

        ################### Custom Business Logic from below #######################################

        total_num_training_jobs = 0
        log.info("Total {} Jobs will be submitted".format(filtered_data.shape[0]))

        meta_data = InferenceMetaDataModel.get("fixedlookupkey")
        s3_pk_mapping_model_prefix_input_path = meta_data.training_prefix_path_from_ssm
              
        for index, row in filtered_data.iterrows():

            total_num_training_jobs = total_num_training_jobs + 1
            if mapping_json_constants['mapping_json_data']['Inference']['all_algorithms']:
                algo_names = mapping_json_constants['mapping_json_data']['Inference']['all_algorithms']

            else:
                algo_names = mapping_json_constants['mapping_json_data']['Inference']['mapping'][row[mapping_id]]
            # algo_names = mapping_json_constants["mapping_json_data"]["Training"][row.region]

            # "smid is same as step function id"
            s3_pk_mappingid_data_input_path = (
                "s3://{}/preprocessing/year={}/month={}/day={}/stepjobid={}/smid={}/pk={}/mapping={}/").format(
                meta_item.s3_bucket_name_shared,
                meta_item.inference_execution_year,
                meta_item.inference_execution_month,
                meta_item.inference_execution_day,
                meta_item.inference_step_job_id,
                meta_item.inference_step_job_id,
                row.pk,
                row[mapping_id])

            aws_batch_job_definition = meta_item.aws_batch_job_definition

            status_input_job = insert_inference_job_def_input_table(pk_mappingid=row.pk + "|" + row[mapping_id],
                                                                    step_job_id=meta_item.inference_step_job_id,
                                                                    usecase_name=meta_item.inference_usecase_name,
                                                                    execution_year=meta_item.inference_execution_year,
                                                                    execution_month=meta_item.inference_execution_month,
                                                                    execution_day=meta_item.inference_execution_day,
                                                                    pkid=row.pk,
                                                                    mapping_id=row[mapping_id],
                                                                    mapping_json_s3_path=meta_item.mapping_json_s3_inference_path,
                                                                    algo_names=algo_names,
                                                                    s3_pk_mappingid_data_input_path=s3_pk_mappingid_data_input_path,
                                                                    s3_output_bucket_name=meta_item.s3_bucket_name_shared,
                                                                    batch_job_definition=aws_batch_job_definition,
                                                                    batch_job_status_overall="TO_BE_CREATED",
                                                                    input_data_set=[
                                                                        analytical_data_path],
                                                                    meta_table_item=meta_data,
                                                                    s3_pk_mapping_model_prefix_input_path=s3_pk_mapping_model_prefix_input_path)

            if not status_input_job:
                raise Exception("Preprocessing failed!")

        pre_processing_end_epoch = int(time.time())

        meta_item.preprocessing_timelaps = Timelaps(start_time=pre_processing_start_epoch,
                                                    end_time=pre_processing_end_epoch)
        meta_item.input_data_set = [analytical_data_path]
        meta_item.features_dq_input_path = analytical_data_path
        # TODO MANOJ - fill alo_names in meta only if mapping_json_constants['mapping_json_data']['Inference']['all_algorithms'] is not empty
        meta_item.algo_names = algo_names
        meta_item.preprocessing_total_batch_jobs = total_num_training_jobs
        meta_item.s3_preprocessing_prefix_output_path = s3_preprocessing_prefix_output_path
        meta_item.save()
        
        logging.info("Inference Preprocessing done!!")
        
    except Exception as error:
        log.error("Error ->{}".format(error))
        traceback.print_exc()
        log.info("Inference Preprocessing Job failed to Complete!")
        sys.exit(1)

