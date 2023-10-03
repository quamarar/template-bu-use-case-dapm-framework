from pynamodb.attributes import UnicodeAttribute, MapAttribute, VersionAttribute
from pynamodb.attributes import UnicodeSetAttribute, NumberAttribute, ListAttribute
from pynamodb.models import Model

"""
https://pynamodb.readthedocs.io/en/stable/quickstart.html
"""

class Timelaps(MapAttribute):
    start_time = NumberAttribute()
    end_time = NumberAttribute()

    def __eq__(self, other):
        return (isinstance(other, InferenceAlgorithmStatus)
                and self.start_time == other.start_time
                and self.end_time == other.end_time
                )

    def __repr__(self):
        return str(vars(self))


class AlgoScore(MapAttribute):
    algo_name = UnicodeAttribute()
    algo_score = NumberAttribute()

    def __eq__(self, other):
        return (isinstance(other, AlgoScore)
                and self.algo_name == other.algo_name
                and self.algo_score == other.algo_score
                )

    def __repr__(self):
        return str(vars(self))


class InferenceMetaDataModel(Model):
    """
    A DynamoDB User
    """
    class Meta:
        read_capacity_units = 100
        write_capacity_units = 100
        # host = "http://localhost:8000"

    @staticmethod
    def setup_model(model, table_name, region):
        model.Meta.table_name = table_name
        model.Meta.region = region

    metaKey = UnicodeAttribute(hash_key=True)
    inference_usecase_name = UnicodeAttribute()
    training_prefix_path_from_ssm = UnicodeAttribute()
    inference_preprocessing_prefix_input_path = UnicodeAttribute(default="")
    pk_column_name = UnicodeAttribute(default="")
    mapping_id_column_name = UnicodeAttribute(default="")
    training_mapping_id_column_name = UnicodeAttribute(default="")
    inference_execution_year = UnicodeAttribute()
    inference_execution_month = UnicodeAttribute()
    inference_execution_day = UnicodeAttribute()
    training_execution_year = UnicodeAttribute()
    training_execution_month = UnicodeAttribute()
    training_execution_day = UnicodeAttribute()
    inference_step_job_id = UnicodeAttribute()
    training_step_job_id = UnicodeAttribute()
    mapping_json_s3_inference_path = UnicodeAttribute()
    mapping_json_s3_training_path = UnicodeAttribute()
    inference_algo_names = UnicodeSetAttribute(null=True)
    training_algo_names = UnicodeSetAttribute(null=True)
    aws_batch_job_definition = UnicodeAttribute()
    aws_batch_job_queue = UnicodeAttribute()
    aws_batch_job_prefixname = UnicodeAttribute()
    s3_bucket_name_analytics_etl=UnicodeAttribute()
    s3_bucket_name_shared = UnicodeAttribute()
    s3_bucket_name_internal = UnicodeAttribute()
    inference_inputtable_name = UnicodeAttribute()
    inference_statetable_name = UnicodeAttribute()
    inference_metatable_name = UnicodeAttribute()
    features_dq_input_path = UnicodeAttribute(default="")
    inference_athenadb_name = UnicodeAttribute()
    inference_athenadb_debug_table_name = UnicodeAttribute()
    inference_athenadb_metadata_table_name = UnicodeAttribute()
    inference_athenadb_inference_table_name = UnicodeAttribute()

    training_athenadb_name = UnicodeAttribute()
    training_athenadb_debug_table_name = UnicodeAttribute()
    training_athenadb_metadata_table_name = UnicodeAttribute()
    training_athena_pred_or_eval_table_name = UnicodeAttribute()

    data_quality_athenadb_name = UnicodeAttribute(default='dataqualityDB')
    data_quality_athenadb_table_name = UnicodeAttribute(default='dataqualitytable')

    commit_id = UnicodeAttribute(default='')
    git_repository_url = UnicodeAttribute(default='')

    preprocessing_total_batch_jobs = NumberAttribute(default=0)
    state_table_total_num_batch_jobs = NumberAttribute(default=0)
    total_num_inference_executed = NumberAttribute(default=0)
    total_numb_batch_job_succeeded = NumberAttribute(default=0)
    total_num_batch_job_failed = NumberAttribute(default=0)
    step_function_start_time = NumberAttribute(default=0)
    step_function_end_time = NumberAttribute(default=0)
    e2e_execution_time = NumberAttribute(default=0)
    gatekeeper_timelaps = Timelaps(default_for_new=Timelaps(start_time=0, end_time=0))
    preprocessing_timelaps = Timelaps(default_for_new=Timelaps(start_time=0, end_time=0))
    aws_batch_submission_timelaps = Timelaps(default_for_new=Timelaps(start_time=0, end_time=0))
    inference_timelaps = Timelaps(default_for_new=Timelaps(start_time=0, end_time=0))
    input_data_set = ListAttribute(of=UnicodeAttribute, default=[])
    s3_inference_prefix_output_path = UnicodeAttribute(default="")
    s3_infer_summary_prefix_output_path = UnicodeAttribute(default="")
    region = UnicodeAttribute(default="ap-south-1")
    training_winning_algo_name = UnicodeAttribute(default="")

    s3_preprocessing_prefix_output_path = UnicodeAttribute(default="")
    training_event_bus_name = UnicodeAttribute()
    email_topic_arn = UnicodeAttribute()


class InferenceAlgorithmStatus(MapAttribute):

    algorithm_name = UnicodeAttribute()
    runid = NumberAttribute()
    algorithm_execution_status = UnicodeAttribute()
    def __eq__(self, other):
        return (isinstance(other, InferenceAlgorithmStatus)
                and self.algorithm_name == other.algorithm_name
                and self.algorithm_execution_status == other.algorithm_execution_status
                and self.runid == other.runid)
    def __repr__(self):
        return str(vars(self))


class InferenceAlgorithmS3OutputPath(MapAttribute):
    algorithm_name = UnicodeAttribute()
    inference_s3_output_path = UnicodeAttribute()


    """inference_s3_output_path = s3://bucketname/inferenceout/year=execution_year
                                           /month=execution_month/day=execution_day/stepjobid=step_job_id/pk=pkid/mapping=mappingid/
                                           batchjobid=batch_job_id/algoname=algorithmname/evaluation.json"""

    def __eq__(self, other):
        return (isinstance(other, InferenceAlgorithmStatus)
                and self.algorithm_name == other.algorithm_name
                and self.inference_s3_output_path == other.inference_s3_output_path
                )

    def __repr__(self):
        return str(vars(self))


class InferenceInputDataModel(Model):
    """
    A DynamoDB User
    """

    class Meta:
        read_capacity_units = 100
        write_capacity_units = 100
        # host = "http://localhost:8000"

    @staticmethod
    def setup_model(model, table_name, region):
        model.Meta.table_name = table_name
        model.Meta.region = region

    # pk|mappingid mappingID is not being made the sort key, as there can be multiple mappingid

    pk_mappingid = UnicodeAttribute(hash_key=True)
    inference_step_job_id = UnicodeAttribute()
    training_step_job_id = UnicodeAttribute()
    inference_usecase_name = UnicodeAttribute()
    inference_execution_year = UnicodeAttribute()
    inference_execution_month = UnicodeAttribute()
    inference_execution_day = UnicodeAttribute()
    training_execution_year = UnicodeAttribute()
    training_execution_month = UnicodeAttribute()
    training_execution_day = UnicodeAttribute()
    pk = UnicodeAttribute()
    mapping_id = UnicodeAttribute()
    mapping_json_s3_inference_path = UnicodeAttribute(default="")
    mapping_json_s3_training_path = UnicodeAttribute(default="")
    inference_input_data_set = ListAttribute(of=UnicodeAttribute, default=[])
    # Algo names to train the individual dates set on
    inference_algo_names = UnicodeSetAttribute()
    training_algo_names = UnicodeSetAttribute()

    # S3 path with complete file name - assumption there is only file per  S3 input Path
    s3_pk_mappingid_data_input_path = UnicodeAttribute()
    s3_pk_mapping_model_prefix_input_path = UnicodeAttribute()
    # bucketname without s3:// prefix
    s3_output_bucket_name = UnicodeAttribute()
    batch_job_definition = UnicodeAttribute()

    batch_job_status_overall = UnicodeAttribute()
    """s3_inference_output_path = s3://bucketname/inference/year=execution_year
                                       /month=execution_month/day=execution_day/stepjobid=step_job_id/pk=pkid/mapping=mappingid/
                                       batchjobid=batch_job_id/algoname=algorithmname"""


class InferenceStateDataModel(Model):
    """
    A DynamoDB User
    """
    class Meta:
        read_capacity_units = 100
        write_capacity_units = 100
        # host = "http://localhost:8000"

    @staticmethod
    def setup_model(model, table_name, region):
        model.Meta.table_name = table_name
        model.Meta.region = region

    # pk|mappingid  mappingID is not being made the sort key, as there can be multiple mappingid
    batchjob_id = UnicodeAttribute(hash_key=True)
    inference_step_job_id = UnicodeAttribute()
    training_step_job_id = UnicodeAttribute()
    inference_usecase_name = UnicodeAttribute()
    inference_execution_year = UnicodeAttribute()
    inference_execution_month = UnicodeAttribute()
    inference_execution_day = UnicodeAttribute()
    training_execution_year = UnicodeAttribute()
    training_execution_month = UnicodeAttribute()
    training_execution_day = UnicodeAttribute()
    mapping_json_s3_inference_path = UnicodeAttribute(default="")
    mapping_json_s3_training_path = UnicodeAttribute(default="")
    inference_input_data_set = ListAttribute(of=UnicodeAttribute, default=[])
    # Algo names to train the individual dates set on
    inference_algo_names = UnicodeSetAttribute()
    training_algo_names = UnicodeSetAttribute()
    pk = UnicodeAttribute()
    mapping_id = UnicodeAttribute()
    # S3 path with complete file name - assumption there is only file per  S3 input Path
    s3_pk_mappingid_data_input_path = UnicodeAttribute()
    s3_pk_mapping_model_prefix_input_path = UnicodeAttribute()
    # bucketname without s3:// prefix
    s3_output_bucket_name = UnicodeAttribute()
    batch_job_definition = UnicodeAttribute()
    batch_job_status_overall = UnicodeAttribute()
    algo_execution_status = ListAttribute(of=InferenceAlgorithmStatus, default=[])
    # fill it will algo info and Path and send it to contaiener ,lookup from Athena will be performed from submit job
    algo_to_model_input_paths_mapping = ListAttribute(of=InferenceAlgorithmS3OutputPath)
    algo_final_run_s3_outputpaths = ListAttribute(of=InferenceAlgorithmS3OutputPath,default=[])
    # S3 path with complete file name - assumption there is only file per  S3 input Pat

    """s3_inference_prefix_output_path = s3://bucketname/inferenceout/year=execution_year
                                       /month=execution_month/day=execution_day/stepjobid=step_job_id/pk=pkid/mapping=mappingid/
                                       batchjobid=batch_job_id"""

    s3_inference_prefix_output_path = UnicodeAttribute()
    s3_infer_summary_prefix_output_path = UnicodeAttribute(default="")
    cur_awsbatchjob_id = UnicodeAttribute()
    rerun_awsbatchjob_id = UnicodeAttribute(default="")
    rerun_awsbatchjob_cw_log_url = UnicodeAttribute(default="")
    first_run_awsbatchjob_cw_log_url = UnicodeAttribute()
    num_runs = NumberAttribute(default=0)
    awsbatch_job_status_overall = UnicodeAttribute()
    awsbatch_triggered_num_runs = NumberAttribute(default=-1)
    last_batch_run_time = NumberAttribute(default=0)
    training_winning_algo_name = UnicodeAttribute(default="")
    version = VersionAttribute()
