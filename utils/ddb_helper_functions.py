import json
import logging
from functools import reduce
import operator
import boto3
import ndjson
import pythena
from botocore.exceptions import ClientError
import s3fs
import os
import time
import re

logging.basicConfig(format='%(asctime)s:%(msecs)03d - %(levelname)-8s [%(filename)s:%(lineno)d] %(message)s',
                    datefmt='%Y-%m-%d:%H:%M:%S',
                    level=logging.INFO)

def extract_hash_from_string(input_string):
    """
    this method extract hash part from the string using regrex

    :param input_string:
    :return: string
    """
    # Define a regular expression pattern to match the hash number
    pattern = r'[a-f0-9]{8}-[a-f0-9]{4}-[a-f0-9]{4}-[a-f0-9]{4}-[a-f0-9]{12}'
    
    # Use re.search to find the first match in the string
    match = re.search(pattern, input_string)
    
    # Check if a match was found and extract the hash number
    if match:
        hash_number = match.group()
        return hash_number
    else:
        return ''

def get_algo_set_of_training_and_inferencing(mapping_json):
    """
    @param mapping_json: mapping json of framework
    """
    train_algos = mapping_json['mapping_json_data']['Training']['all_algorithms']
    if mapping_json['mapping_json_data']['Inference']['all_algorithms']:
        inference_algos = mapping_json['mapping_json_data']['Inference']['all_algorithms']

    else:
        inference_algos = reduce(operator.concat, mapping_json['mapping_json_data']['Inference']['mapping'].values())
    return set(train_algos), set(inference_algos)


def get_mapping_column_of_training_and_inferencing(mapping_json):
    """
    @param mapping_json: mapping json of framework
    """
    train_mapping_column = mapping_json['mapping_json_data']['Training']['mappingColumn']
    Inference_mapping_column = mapping_json['mapping_json_data']['Inference']['mappingColumn']
    return train_mapping_column, Inference_mapping_column


def email_sns(sns_client, topic_arn, message, subject):
    """
    Purpose: Send email notification to the user regarding the failure of DQ checks
    param : topic_arn: ARN of the email topic
    param : message: email message
    param : subject: email subject
    return: None
    """
    response = sns_client.publish(
        TopicArn=topic_arn,
        Message=message,
        Subject=subject)
    return response


def is_train_superset_of_inference_algo(function_to_get_sets, mapping_json):
    """
    @param function_to_get_sets: method to get unique set of all algorithms for train and inference
    @param mapping_json: mapping json of framework
    """
    train_set, inference_set = function_to_get_sets(mapping_json)
    return train_set.issuperset(inference_set)


def delete_folder_from_s3(boto_s3_resource, bucket, marker):
    """

    :param boto_s3_resource: boto3.resource('s3) ->object
    :param bucket: s3 bucket name
    :param marker: Marker is where you want Amazon S3 to start listing from. Amazon S3 starts listing after this specified key.
    :return:
    """

    # s3 = boto3.resource('s3')s3
    bucket = boto_s3_resource.Bucket(bucket)
    for folder in bucket.objects.filter(Prefix=marker):
        # print(o.delete())
        folder.delete()

    return


def fetch_all_records(ddb_model) -> list:
    """
    :param ddb_model:
    :return:
    """
    return ddb_model.scan()


def delete_ddb_table(ddb_model):
    """

    :param ddb_model:
    :return:
    """
    try:
        ddb_model.delete_table()

    except Exception as error:
        logging.error(error)
        print("Error- {}".format(error))
        return 1
    finally:
        # TODO- send sns was not able to cleanup
        pass
    return 0


def dump_data_to_s3(s3_ouput_bucket, s3_output_object_name, ddb_model):
    """
    Method fetches data from DDB table and dumps to S3 bucket
    :param s3_ouput_bucket:
    :param s3_output_object_name:
    :param ddb_model:
    :return:
    """
    # temp_file = "table_data.json"
    # out_file = open(temp_file, 'w')
    logging.info("Dumping State Table to S3 {}".format(s3_output_object_name))
    all_records = ddb_model.scan()
    all_data = [json.loads(record.to_json()) for record in all_records]

    if upload_file(data=ndjson.dumps(all_data), bucket_name=s3_ouput_bucket, object_name=s3_output_object_name):
        logging.info("Dump State Table to {}".format("Complete"))


def upload_file(data, bucket_name, object_name) -> bool:
    """
    Methods helps to upload data to s3
    :param data:
    :param bucket_name:
    :param object_name:
    :return:
    """
    logging.info('File Uploading -Bucket {} Key {}'.format(bucket_name, object_name))
    try:
        s3 = boto3.resource('s3')
        s3object = s3.Object(bucket_name, object_name)

        s3object.put(
            Body=(bytes(data.encode('UTF-8')))
            )
        logging.info('File Upload Successful -Bucket {} Key {}'.format(bucket_name, object_name))
    except ClientError as error:
        logging.info('File Upload Bucket {} Key {} ERROR {}'.format(bucket_name, object_name, error))
        return False
    return True


def delete_table_record(ddb_model, column_object, column_value) -> bool:
    """

    :param ddb_model: Table object ex: InputTable
    :param column_object: primary key object InputTable.skuMapping
    :param column_value: primary key value
    :return:
    """
    try:

        context = ddb_model.get(column_value)
        context.refresh()
        context.delete(condition=(column_object == column_value))

    except Exception as error:
        logging.error(error)
        raise Exception("Exception while executing delete_table_record")
    return True


def get_batch_container_image_arn(batch_definiton) -> str:
    batch_client = boto3.client('batch')
    response = batch_client.describe_job_definitions(
        jobDefinitions=[
            batch_definiton,
            ],
        maxResults=1,

        )
    if 0 == len(response):
        logging.warning("No data retrieved")
        return ''
    else:
        container_image_arn = response['jobDefinitions'][0]['containerProperties']['image']
        return container_image_arn


def submit_inference_aws_batch_job(
        boto3_client,
        s3_inferencing_prefix_input_path,
        s3_inferencing_prefix_output_path,
        inference_metatable_name,
        mapping_id,
        s3_approved_model_prefix_path,
        pk_id,
        region,
        aws_batch_job_name,
        aws_batch_job_queue,
        aws_batch_job_definition,
        job_id='empty',
        aws_batch_compute_scale_factor=1
        ) -> tuple:
    """
    @param boto3_client:
    @type boto3_client:
    @param s3_inferencing_prefix_input_path:
    @type s3_inferencing_prefix_input_path:
    @param s3_inferencing_prefix_output_path:
    @type s3_inferencing_prefix_output_path:
    @param inference_metatable_name:
    @type inference_metatable_name:
    @param mapping_id:
    @type mapping_id:
    @param s3_approved_model_prefix_path:
    @type s3_approved_model_prefix_path:
    @param pk_id:
    @type pk_id:
    @param region:
    @type region:
    @param aws_batch_job_name:
    @type aws_batch_job_name:
    @param aws_batch_job_queue:
    @type aws_batch_job_queue:
    @param aws_batch_job_definition:
    @type aws_batch_job_definition:
    @param job_id:
    @type job_id:
    @param aws_batch_compute_scale_factor:
    @type aws_batch_compute_scale_factor:
    @return:
    @rtype:
    """
    cur_job_id = ""
    try:
        boto3_client = boto3_client
        assert aws_batch_compute_scale_factor > 0, "AWS Batch Computation Scale Factor Can't be Zero or Negative"

        custom_command = [
            '--s3_inferencing_data_input_path', s3_inferencing_prefix_input_path,
            '--s3_inferencing_prefix_output_path', s3_inferencing_prefix_output_path,
            '--inference_metatable_name', inference_metatable_name,
            '--pk_id', pk_id,
            '--mapping_id', mapping_id,
            '--prev_batch_job_id', job_id,
            '--region', region
            ]

        if aws_batch_compute_scale_factor > 1:

            _, compute_requirement = get_aws_job_status_and_compute_requirement(batchjob_id=job_id,
                                                                                boto3_client=boto3_client)
            vcpu = compute_requirement[0]['value']
            memory = compute_requirement[1]['value']
            response = boto3_client.submit_job(
                jobName=aws_batch_job_name,
                jobQueue=aws_batch_job_queue,
                jobDefinition=aws_batch_job_definition,
                containerOverrides={
                    'command': custom_command,
                    'vcpus': int(vcpu) * aws_batch_compute_scale_factor,
                    'memory': int(memory) * aws_batch_compute_scale_factor,
                    },
                retryStrategy={
                    'attempts': 1
                    }
                )
            cur_job_id = response["jobId"]

        else:

            response = boto3_client.submit_job(
                jobName=aws_batch_job_name,
                jobQueue=aws_batch_job_queue,
                jobDefinition=aws_batch_job_definition,
                containerOverrides={
                    'command': custom_command
                    },
                retryStrategy={
                    'attempts': 1
                    }
                )
            cur_job_id = response["jobId"]

    except Exception as error:
        logging.error("Error- {}".format(error))
        # print("Error- {}".format(error))
        raise Exception("submit_inference_aws_batch_job failed raising exception")

    return True, cur_job_id, aws_batch_job_definition


def submit_aws_batch_job(boto3_client, algo_names_list,
                         s3_pk_mappingid_data_input_path,
                         s3_training_prefix_output_path,
                         s3_pred_or_eval_prefix_output_path,
                         train_metatable_name,
                         pk,
                         mapping_id,
                         aws_batch_job_name,
                         aws_batch_job_queue,
                         aws_batch_job_definition,
                         region,
                         aws_batch_compute_scale_factor=1,
                         job_id="empty"
                         ) -> tuple:
    """
    adding job to t
    :param s3_pk_mappingid_data_input_path:
    :param s3_training_prefix_output_path:
    :param s3_pred_or_eval_prefix_output_path:
    :param train_metatable_name:
    :param pk:
    :param boto3_client:
    :param aws_batch_compute_scale_factor:
    :param aws_batch_job_name:
    :param aws_batch_job_queue:
    :param aws_batch_job_definition:
    :param job_id:
    :return: return tuple with exit code , job_id
    """

    cur_job_id = ""
    try:

        boto3_client = boto3_client
        assert aws_batch_compute_scale_factor > 0, "AWS Batch Computation Scale Factor Can't be Zero or Negative"

        if aws_batch_compute_scale_factor > 1:
            # TODO- need to write logic here
            # describe_job_id = "{}".format(job_id)
            # response = boto3_client.describe_jobs(
            #     jobs=[
            #         describe_job_id
            #     ]
            # )
            _, compute_requirement = get_aws_job_status_and_compute_requirement(batchjob_id=job_id,
                                                                                boto3_client=boto3_client)
            vcpu = compute_requirement[0]['value']
            memory = compute_requirement[1]['value']
            response = boto3_client.submit_job(
                jobName=aws_batch_job_name,
                jobQueue=aws_batch_job_queue,
                jobDefinition=aws_batch_job_definition,
                containerOverrides={
                    'command': [
                        '--s3_pk_mappingid_data_input_path', s3_pk_mappingid_data_input_path,
                        '--s3_training_prefix_output_path', s3_training_prefix_output_path,
                        '--s3_pred_or_eval_prefix_output_path', s3_pred_or_eval_prefix_output_path,
                        '--train_metatable_name', train_metatable_name,
                        '--prev_batch_job_id', job_id,
                        '--pk_id', pk,
                        '--mapping_id', mapping_id,
                        '--region', region
                        ],
                    'vcpus': int(vcpu) * aws_batch_compute_scale_factor,
                    'memory': int(memory) * aws_batch_compute_scale_factor,
                    },
                retryStrategy={
                    'attempts': 1
                    }
                )
            cur_job_id = response["jobId"]

        else:

            response = boto3_client.submit_job(
                jobName=aws_batch_job_name,
                jobQueue=aws_batch_job_queue,
                jobDefinition=aws_batch_job_definition,
                containerOverrides={
                    'command': [
                        '--s3_pk_mappingid_data_input_path', s3_pk_mappingid_data_input_path,
                        '--s3_training_prefix_output_path', s3_training_prefix_output_path,
                        '--s3_pred_or_eval_prefix_output_path', s3_pred_or_eval_prefix_output_path,
                        '--train_metatable_name', train_metatable_name,
                        '--prev_batch_job_id', job_id,
                        '--pk_id', pk,
                        '--mapping_id', mapping_id,
                        '--region', region
                        ]
                    },
                retryStrategy={
                    'attempts': 1
                    }
                )
            cur_job_id = response["jobId"]

    except Exception as error:
        logging.error("Error- {}".format(error))
        # print("Error- {}".format(error))
        raise Exception("submit_aws_batch_job failed raising exception")

    return True, cur_job_id, aws_batch_job_definition


def get_aws_job_status_and_compute_requirement(batchjob_id, boto3_client):
    """

    :param batchjob_id:
    :param boto3_client:
    :return: string of status
    """
    describe_job_id = "{}".format(batchjob_id)
    print(describe_job_id)
    response = boto3_client.describe_jobs(
        jobs=[
            describe_job_id
            ]
        )
    status = response["jobs"][0]["status"]
    compute_requirement = response["jobs"][0]['container']['resourceRequirements']
    print("Job {} Status: {}".format(batchjob_id, status))
    return status, compute_requirement


def get_job_logstream(batchjob_id, boto3_client):
    """

    :param batchjob_id:
    :param boto3_client:
    :return:
    """
    describe_job_id = "{}".format(batchjob_id)
    response = boto3_client.describe_jobs(
        jobs=[
            describe_job_id
            ]
        )
    print("log response: {}".format(response))
    log_stream_name = response["jobs"][0]['attempts'][0]['container']['logStreamName']
    print("logStreamName: {}".format(log_stream_name))
    return log_stream_name


def read_json_from_s3(source_path, s3_boto3_client=None) -> json:
    """
    :param s3_boto3_client:
    :param source_path:
    :return: json from mapping.json
    """
    logging.info("Reading Json from source path {}".format(source_path))
    try:

        fs = s3fs.S3FileSystem()
        with fs.open(source_path) as ff:
            data = json.load(ff)
            logging.info("json data {}".format(data))
        return data
    except Exception as error:
        logging.error("Error(reading_mapping_json): {}".format(error))
        raise Exception("read_json_from_s3 function failed ")


def copy_mapping_json(source_path, destination_s3_bucket, destination_s3_key) -> json:
    """

    :param s3_boto3_client:
    :param source_path:
    :param destination_s3_bucket:
    :param destination_s3_key:
    :return: json from mapping.json ,  destination_path
    """

    try:

        fs = s3fs.S3FileSystem()
        with fs.open(source_path) as ff:
            data = json.load(ff)
        if not destination_s3_bucket.startswith('s3://'):
            destination_s3_bucket = 's3://' + destination_s3_bucket

        destination_path = os.path.join(destination_s3_bucket, destination_s3_key)
        # print("destination path {}".format(destination_path))
        logging.info("destination path {}".format(destination_path))
        with fs.open(destination_path, 'w') as ff:
            json.dump(data, ff)

        return data, destination_path

    except Exception as error:
        logging.error("Error(copy_mapping_json): {}".format(error))
        print("Error: {}".format(error))
        raise Exception("copy_mapping_json function failed ")


def update_ssm_store(ssm_parameter_name, value) -> None:
    logging.info("updating ssm parameter {}".format(ssm_parameter_name))
    try:

        ssm_client = boto3.client('ssm')

        ssm_client.put_parameter(
            Name=ssm_parameter_name,
            Description='status complete',
            Value=value,
            Overwrite=True
            )
    except Exception as error:
        logging.error("updating ssm parameter {} failed".format(ssm_parameter_name))
        raise Exception(" Error while updating ssm parameter: {}".format(error))


def read_ssm_store(ssm_parameter_name) -> dict:
    logging.info("fetch ssm parameter value- {}".format(ssm_parameter_name))
    ssm_client = boto3.client('ssm')

    response = ssm_client.get_parameter(
        Name=ssm_parameter_name,

        )
    return response


def read_athena_table_data(athena_db_name, region_name, db_query):
    # algo_final_run_s3outputpaths
    # year
    # month
    # day
    # pk
    # mapping_id
    """
    this method fetches data from Athena Table
    @:param athena_db_name:
    @:param region_name:
    @:param db_query:
    :return: pandas dataframe
    """

    logging.info("Running query on Athena Table - {}".format(athena_db_name))

    session = boto3.session.Session()
    athena_client = pythena.Athena(database=athena_db_name, session=session, region=region_name)
    validation_df, execution_id = athena_client.execute(query=db_query)

    return validation_df


def query_execution_status(execution_id, athena_client):
    """
    Purpose: Check the athena execution status of athena query and keep on trying if the query is in pending or running state
    param : execution_id: execution id of the executed athena query
    param : athena_client: athena client object
    return: query_status: query status of the query
    """
    query_status = athena_client.get_query_status(execution_id)
    if query_status == 'QUEUED' or query_status == 'RUNNING':
        print(f"Sleep for 10 seconds ")
        time.sleep(10)
        query_execution_status(execution_id, athena_client)
    elif query_status == 'SUCCEEDED':
        print(f"Completed ")
        return query_status
    elif query_status == 'FAILED' or query_status == 'CANCELLED':
        print(f"Failed ")
        return query_status

## for future use
# def get_state_machine_executions(sfn_boto_client, sf_arn, status_filter="RUNNING", max_results=10):
#     response = sfn_boto_client.list_executions(
#         stateMachineArn=sf_arn,
#         statusFilter=status_filter,
#         maxResults=max_results
#         )
#     return response['executions']
