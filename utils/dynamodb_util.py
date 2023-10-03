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
        return (isinstance(other, TrainingAlgorithmStatus)
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


class TrainingMetaDataModel(Model):
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
    usecase_name = UnicodeAttribute()
    pk_column_name = UnicodeAttribute(default="")
    mapping_id_column_name = UnicodeAttribute(default="")
    execution_year = UnicodeAttribute()
    execution_month = UnicodeAttribute()
    execution_day = UnicodeAttribute()
    step_job_id = UnicodeAttribute()
    mapping_json_s3_path = UnicodeAttribute(default="")
    algo_names = UnicodeSetAttribute(null=True)
    aws_batch_job_definition = UnicodeAttribute()
    aws_batch_job_queue = UnicodeAttribute()
    aws_batch_job_prefixname = UnicodeAttribute()
    s3_bucket_name_shared = UnicodeAttribute()
    s3_bucket_name_internal = UnicodeAttribute()
    s3_bucket_name_analytics_etl=UnicodeAttribute()
    train_inputtable_name = UnicodeAttribute()
    train_statetable_name = UnicodeAttribute()
    train_metatable_name = UnicodeAttribute()
    athenadb_name = UnicodeAttribute()
    athena_pred_or_eval_table_name = UnicodeAttribute()
    athenadb_debug_table_name = UnicodeAttribute()
    athenadb_metadata_table_name = UnicodeAttribute()
    athenadb_evaluation_summary_table = UnicodeAttribute()
    preprocessing_total_batch_jobs = NumberAttribute(default=0)
    state_table_total_num_batch_jobs = NumberAttribute(default=0)
    total_num_models_created = NumberAttribute(default=0)
    total_numb_batch_job_succeeded = NumberAttribute(default=0)
    total_num_batch_job_failed = NumberAttribute(default=0)
    step_function_start_time = NumberAttribute(default=0)
    step_function_end_time = NumberAttribute(default=0)
    e2e_execution_time = NumberAttribute(default=0)
    gatekeeper_timelaps = Timelaps(default_for_new=Timelaps(start_time=0, end_time=0))
    preprocessing_timelaps = Timelaps(default_for_new=Timelaps(start_time=0, end_time=0))
    aws_batch_submission_timelaps = Timelaps(default_for_new=Timelaps(start_time=0, end_time=0))
    model_creation_pred_or_eval_timelaps = Timelaps(default_for_new=Timelaps(start_time=0, end_time=0))
    eval_summary_timelaps = Timelaps(default_for_new=Timelaps(start_time=0, end_time=0))
    features_dq_input_path=UnicodeAttribute(default="")
    s3_preprocessing_prefix_output_path = UnicodeAttribute(default="")
    s3_training_prefix_output_path = UnicodeAttribute(default="")
    s3_pred_or_eval_prefix_output_path = UnicodeAttribute(default="")
    s3_eval_summary_prefix_output_path = UnicodeAttribute(default="")
    region = UnicodeAttribute(default="ap-south-1")
    algo_with_highest_score = UnicodeAttribute(default="")
    algo_score = ListAttribute(of=AlgoScore, default=[])
    model_package_group_name = UnicodeAttribute()
    commit_id = UnicodeAttribute(default="")
    repository = UnicodeAttribute(default="")
    training_event_bus_name = UnicodeAttribute(default="")
    email_topic_arn = UnicodeAttribute(default="")


class TrainingAlgorithmStatus(MapAttribute):
    algorithm_name = UnicodeAttribute()
    runid = NumberAttribute()
    algorithm_execution_status = UnicodeAttribute()

    def __eq__(self, other):
        return (isinstance(other, TrainingAlgorithmStatus)
                and self.algorithm_name == other.algorithm_name
                and self.algorithm_execution_status == other.algorithm_execution_status
                and self.runid == other.runid)

    def __repr__(self):
        return str(vars(self))


class TrainingAlgorithmS3OutputPath(MapAttribute):
    algorithm_name = UnicodeAttribute()
    model_s3_output_path = UnicodeAttribute()
    pred_or_eval_s3_output_path = UnicodeAttribute()

    """model_s3_output_path = s3://bucketname/training/year=execution_year
                                       /month=execution_month/day=execution_day/stepjobid=step_job_id/pk=pkid/mapping=mappingid/
                                       batchjobid=batch_job_id/algoname=algorithmname/model.tar.gz

       pred_or_eval_s3_output_path = s3://bucketname/pred_or_eval/year=execution_year
                                           /month=execution_month/day=execution_day/stepjobid=step_job_id/pk=pkid/mapping=mappingid/
                                           batchjobid=batch_job_id/algoname=algorithmname/evaluation.json
    """

    def __eq__(self, other):
        return (isinstance(other, TrainingAlgorithmStatus)
                and self.algorithm_name == other.algorithm_name
                and self.model_s3_output_path == other.model_s3_output_path
                and self.evaluation_s3_output_path == other.evaluation_s3_output_path
                )

    def __repr__(self):
        return str(vars(self))


class TrainInputDataModel(Model):
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
    step_job_id = UnicodeAttribute()
    usecase_name = UnicodeAttribute()
    execution_year = UnicodeAttribute()
    execution_month = UnicodeAttribute()
    execution_day = UnicodeAttribute()
    pk = UnicodeAttribute()
    mapping_id = UnicodeAttribute()
    mapping_json_s3_path = UnicodeAttribute(default="")
    input_data_set = ListAttribute(of=UnicodeAttribute, default=[])
    # Algo names to train the individual dates set on
    algo_execution_status = ListAttribute(of=TrainingAlgorithmStatus, default=[])
    algo_names = UnicodeSetAttribute()

    # S3 path with complete file name - assumption there is only file per  S3 input Path
    s3_pk_mappingid_data_input_path = UnicodeAttribute()
    # bucketname without s3:// prefix
    s3_output_bucket_name = UnicodeAttribute()
    batch_job_definition = UnicodeAttribute()

    batch_job_status_overall = UnicodeAttribute()
    """s3_training_output_path = s3://bucketname/usecasename/training/year=execution_year
                                       /month=execution_month/day=execution_day/stepjobid=step_job_id/pk=pkid/mapping=mappingid/
                                       batchjobid=batch_job_id/algoname=algorithmname

       pred_or_eval_s3_output_path = s3://bucketname/pred_or_eval/year=execution_year
                                           /month=execution_month/day=execution_day/stepjobid=step_job_id/pk=pkid/mapping=mappingid/
                                           batchjobid=batch_job_id/algoname=algorithmname/evaluation.json"""


class TrainStateDataModel(Model):
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
    step_job_id = UnicodeAttribute()
    pk = UnicodeAttribute()
    mapping_id = UnicodeAttribute()
    mapping_json_s3_path = UnicodeAttribute(default="")
    usecase_name = UnicodeAttribute()
    input_data_set = ListAttribute(of=UnicodeAttribute, default=[])
    # Algo names to train the individual dates set on
    algo_execution_status = ListAttribute(of=TrainingAlgorithmStatus, default=[])
    algo_names = UnicodeSetAttribute()
    algo_final_run_s3outputpaths = ListAttribute(of=TrainingAlgorithmS3OutputPath, default=[])
    # S3 path with complete file name - assumption there is only file per  S3 input Path
    s3_pk_mappingid_data_input_path = UnicodeAttribute()

    batch_job_definition = UnicodeAttribute()

    """s3_training_output_path = s3://bucketname/usecasename/training/year=execution_year
                                       /month=execution_month/day=execution_day/stepjobid=step_job_id/pk=pkid/mapping=mappingid/
                                       batchjobid=batch_job_id"""

    s3_training_prefix_output_path = UnicodeAttribute()

    """pred_or_eval_s3_output_path = s3://bucketname/pred_or_eval/year=execution_year
                                           /month=execution_month/day=execution_day/stepjobid=step_job_id/pk=pkid/mapping=mappingid/
                                           batchjobid=batch_job_id/"""

    s3_pred_or_eval_prefix_output_path = UnicodeAttribute()

    # """s3_eval_summary_prefix_output_path = s3://bucketname/eval_summary/year=execution_year
    #                                           /month=execution_month/day=execution_day/stepjobid=step_job_id/pk=pkid/mapping=mappingid/
    #                                           batchjobid=batch_job_id/"""
    #
    # # s3_eval_summary_prefix_output_path = UnicodeAttribute(default="")

    cur_awsbatchjob_id = UnicodeAttribute()
    rerun_awsbatchjob_id = UnicodeAttribute(default="")
    rerun_awsbatchjob_cw_log_url = UnicodeAttribute(default="")
    first_run_awsbatchjob_cw_log_url = UnicodeAttribute()
    num_runs = NumberAttribute(default=0)
    awsbatch_job_status_overall = UnicodeAttribute()
    awsbatch_triggered_num_runs = NumberAttribute(default=-1)
    last_batch_run_time = NumberAttribute(default=0)
    version = VersionAttribute()
