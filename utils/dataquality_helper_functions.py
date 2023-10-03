from awsgluedq.transforms import EvaluateDataQuality
from awsglue.dynamicframe import DynamicFrame
from awsglue.transforms import SelectFromCollection
import boto3
from datetime import date,datetime
from pyspark.sql.functions import col,when
import logging



log = logging.getLogger(__name__)
logging.basicConfig(format='%(asctime)s :: %(message)s', level=logging.INFO)
logging.info("info")


def read_csv_for_dynamic_frame(glueContext, spark, input_file_path):
    input_data_df = spark.read.format("csv").option("header", "true").option("inferSchema", "true").load(
        input_file_path)

    input_data_dyf = DynamicFrame.fromDF(input_data_df, glueContext, "input_data_dyf")
    log.info(f"Created glue dynamic dataframe reading the file from {input_file_path}")
    return input_data_dyf


def read_parquet_for_dynamic_frame(glueContext, spark, input_file_path):
    input_data_df = spark.read.format("parquet").option("header", "true").option("inferSchema", "true").load(
        input_file_path)

    input_data_dyf = DynamicFrame.fromDF(input_data_df, glueContext, "input_data_dyf")
    log.info(f"Created glue dynamic dataframe reading the file from {input_file_path}")
    return input_data_dyf


def evaluate_dq_with_dashboard_metric(glueContext,spark, dashboard_additional_metric_dataset, dashboard_additional_metric_column,
                                      input_data_dyf, stepjobid, input_job_identifier, year, month, day, usecase_name,
                                      job_frequency,solution_type,solution_category):
    ##################################################################################################################
    #### Dataframe to create dq ruleset and provide the merging dataframe between dq result and threshold

    variableruleset = spark.createDataFrame(data=dashboard_additional_metric_dataset,
                                            schema=dashboard_additional_metric_column)
    log.info("Spark Dataframe created containing dq ruleset and additional dashboard metrices")
    variableruleset.show(truncate=False)
    variableruleset.createOrReplaceTempView("variableruleset")

    ##################################################################################################################
    #### Generating the input dqresultset which needs to be fetched from dashboard_metric_dataset ( list of dictionary)
    dqstring = ''
    for i in dashboard_additional_metric_dataset:
        dqstring += i[5]
        dqstring += ',\n'
    dqstring = dqstring[:-1]
    dqstring = dqstring.rstrip(dqstring[-1])
    log.info(f"Inputs for creating ruleset : {dqstring}")

    ########################################################################
    #### Create the rule set( Business DQ checks) using DQDL
    dqruleset = f"""
        Rules = [
            {dqstring}
            ]
    """
    log.info(f"dqruleset: {dqruleset}")
    EvaluateDataQuality_ruleset = dqruleset

    #########################################################################
    ## evaluation of rules against given ruleset
    detailed_result_dyf, consolidated_dq_result_dyf = evaluate_dq_rules(glueContext, spark, input_data_dyf,
                                                                        dqruleset, stepjobid, input_job_identifier,
                                                                        year, month, day)
    log.info("Evaluated the rules")

    ########################################################################
    #### DQ outcome
    dqoutcome = consolidated_dq_result_dyf.toDF()
    dqoutcome.createOrReplaceTempView("dqoutcome")

    ########################################################################
    """
    Merged 2 dataframes to get data quality required output
    1. variableruleset (Joining columns between out come and additional parameters )
    2. dqoutcomes (DQ checks outcomes for all the validation rules)
    """

    final_df = spark.sql(f"""Select vrs.*,source,
                            outcome,FailureReason,EvaluatedMetrics,actual_value,PassCnt, FailCnt,AuditTimestamp ,
                            '{job_frequency}' as job_frequency, 
                            '{solution_type}' as solution_type,'{solution_category}' as solution_category
                            from variableruleset vrs
                            inner join dqoutcome dqo on dqo.rule = vrs.rule

                            """)

    final_consolidated_dq_dyf = DynamicFrame.fromDF(final_df, glueContext, "final_consolidated_dq_dyf")
    log.info("Final Dataframe calculated")
    return final_consolidated_dq_dyf, detailed_result_dyf, final_df


def read_input_athena_table(glueContext,spark, source_database_name, source, column_list):
    # Captured glue dataframe
    source_data_dyf = glueContext.create_dynamic_frame.from_catalog(database=source_database_name, table_name=source)
    # Converted glue dataframe into spark dataframe
    source_data_df = source_data_dyf.toDF()
    # Converted spark dataframe into temp view
    source_data_df.createOrReplaceTempView("source_data_df")

    input_df_sql = f"select {column_list} from source_data_df"
    log.info(f"SQL for the source input :{input_df_sql}")
    # Capture the additional columns when needed
    input_df = spark.sql(input_df_sql)

    log.info("Got the required column for creating dataset for validation ")
    # Convert spark dataframe into dynamic dataframe
    input_data_dyf = DynamicFrame.fromDF(input_df, glueContext, "input_data_dyf")

    log.info("Generated the dynamic df from input data")
    # input_data_dyf.toDF().show()

    return input_data_dyf

def gluedf_s3_loading(glueContext,dataframe_to_write, s3_path, athena_db,
                      athena_table):
    list_partition_key= ["year","month","day"]
    compression = "snappy"
    fileformat = "glueparquet"

    outcome = glueContext.getSink(
        path=s3_path,
        connection_type="s3",
        updateBehavior="UPDATE_IN_DATABASE",
        partitionKeys=list_partition_key,
        compression=compression,
        enableUpdateCatalog=True
    )
    outcome.setCatalogInfo(
        catalogDatabase=athena_db, catalogTableName=athena_table
    )
    outcome.setFormat(fileformat)
    outcome.writeFrame(dataframe_to_write)
    log.info(f"{dataframe_to_write} loaded in {s3_path} with catalog update of {athena_db}.{athena_table}")


def evaluate_only(dynamic_dataframe,dataquality_ruleset):
    evaluate_dataquality_multiframe = EvaluateDataQuality().process_rows(
        frame=dynamic_dataframe,
        ruleset=dataquality_ruleset,
        publishing_options={
            "dataQualityEvaluationContext": "EvaluateDataQualityMultiframe",
            "enableDataQualityCloudWatchMetrics": False,
            "enableDataQualityResultsPublishing": False,
        },
        additional_options={"performanceTuning.caching": "CACHE_NOTHING"},
    )

    return evaluate_dataquality_multiframe

def get_consolidated_outcomes_dq_evaluation(evaluate_dataquality_multiframe):
    rule_outcomes = SelectFromCollection.apply(
        dfc=evaluate_dataquality_multiframe,
        key="ruleOutcomes"
    )

    return rule_outcomes


def evaluate_dq_rules(glueContext,spark,dynamic_dataframe,dataquality_ruleset,workflow_id,source,year,month,day):


    evaluate_dataquality_multiframe=evaluate_only(dynamic_dataframe, dataquality_ruleset)

    ########################################################################
    #### Get the consolidated rule outcomes against rule set
    log.info("Get the consolidated rule outcomes against rule set ")

    rule_outcomes=get_consolidated_outcomes_dq_evaluation(evaluate_dataquality_multiframe)

    rule_outcomes_df = rule_outcomes.toDF()
    rule_outcomes_df.createOrReplaceTempView("rule_outcomes_df")

    ########################################################################
    #### Get the rowlevel outcomes against rule set
    log.info("Get the rowlevel outcomes ")
    rowlevel_outcomes = SelectFromCollection.apply(
        dfc=evaluate_dataquality_multiframe,
        key="rowLevelOutcomes"
    )

    rowlevel_outcomes_df = rowlevel_outcomes.toDF()
    rowlevel_outcomes_df.createOrReplaceTempView("rowlevel_outcomes_df")

    ########################################################################
    #### Update the  rowlevel outcomes in specific structure
    log.info("Update the  rowlevel outcomes in specific structure ")
    sql_detailed_result_query = f"""
    select '{workflow_id}' as corelation_id ,* , '{year}' as year, '{month}' as month , '{day}' as day , current_timestamp() as AuditTimestamp
    from rowlevel_outcomes_df 
    """

    detailed_result_df = spark.sql(sql_detailed_result_query)

    detailed_result_dyf = DynamicFrame.fromDF(detailed_result_df, glueContext, "detailed_result_dyf")

    ########################################################################
    #### Update the  rowlevel outcomes in specific structure
    log.info("Consolidated result of all the DQ checks")

    sql_consolidated_result_query = f"""
        select  '{workflow_id}' as corelation_id ,
        '{source}' as  source, 
        case when (length(cast(evaluatedmetrics as string)) - length(replace(cast(evaluatedmetrics as string),'->',''))) > 2 then 'NA' else  substring((substring_index(cast(evaluatedmetrics as string), '->', -1)), 1, length(substring_index(cast(evaluatedmetrics as string), '->', -1))-1) end as actual_value,
        t1.Rule , t1.Outcome, t1.FailureReason,cast (t1.EvaluatedMetrics as string) EvaluatedMetrics,
        PassCnt, FailCnt ,'{year}' as year, '{month}' as month , '{day}' as day  , current_timestamp() as AuditTimestamp
        from 
        rule_outcomes_df t1
        left join 
        (select pass_header as Rule,nvl(pass_cnt,0) as PassCnt,nvl(fail_cnt,0) as FailCnt from
            (select count(*) pass_cnt, pass_header from 
            ( select  explode(dataqualityrulespass) as pass_header from rowlevel_outcomes_df )
            group by pass_header) p 
        full outer join 
            (select count(*) fail_cnt , fail_header from 
            ( select  explode(dataqualityrulesfail) as fail_header from rowlevel_outcomes_df )
            group by fail_header) f 
        on p.pass_header  = f.fail_header) t2
        on t1.Rule = t2.Rule
    """

    consolidated_dq_result_df = spark.sql(sql_consolidated_result_query)
    consolidated_dq_result_df.show(truncate=False)

    consolidated_dq_result_dyf = DynamicFrame.fromDF(consolidated_dq_result_df, glueContext, "consolidated_dq_result_dyf")

    return detailed_result_dyf , consolidated_dq_result_dyf

def get_completed_dqjobs_cnt_from_worflowid(athena_client,athena_db,athena_table,corelation_id):

    query_dq_completed_job_cnt = f"""select count(1) completed_job_cnt from (
    SELECT distinct source FROM {athena_db}.{athena_table} where corelation_id = '{corelation_id}')"""

    log.info(f"Query to get the count of Dq jobs completed in a workflow: {query_dq_completed_job_cnt}")
    (validation_df, execution_id) = athena_client.execute(query=query_dq_completed_job_cnt)
    completed_job_cnt = validation_df['completed_job_cnt'].tolist()[0]

    return completed_job_cnt

def get_expected_dqjob_cnt_ssm_store(parameter_name,region):
    ssm_client = boto3.client('ssm', region_name=region)

    response = ssm_client.get_parameter(
        Name=parameter_name,
        WithDecryption=True | False
    )
    return (response['Parameter']['Value'])

def get_email_message(athena_client,athena_db,athena_table,corelation_id):

    overall_dq_status = f"""select source, (cast(day as varchar)||'-'||cast(month as varchar)||'-'||cast(year as varchar)) run_date,
    case when val >= 1 then 'Failed' else 'Passed' end result from 
    (SELECT source,day,month,year,
    sum(case when outcome = 'Passed' then 0 
    when outcome = 'Failed' then 1 end) val
    FROM {athena_db}.{athena_table} where corelation_id = '{corelation_id}'
    group by source,day,month,year)"""

    log.info(f"Query to get the email message content :{overall_dq_status}")

    (overall_dq_status_df, execution_id) = athena_client.execute(query=overall_dq_status)
    message=overall_dq_status_df.to_string()
    return message


def after_processing_generate_dq_overall_status_consolidated(glueContext,spark, dynamic_dataframe, corelation_id,
                                                             dq_overall_status_rule_name, year, month, day):
    consolidated_dq_result_df = dynamic_dataframe.toDF()
    consolidated_dq_result_df.show()
    consolidated_dq_result_df.createOrReplaceTempView("consolidated_dq_result_df")
    
    

    final_dq_cal_query = f""" select '{corelation_id}' as corelation_id, source ,null as actual_value,'{dq_overall_status_rule_name}' as rule,
                                case when con_status >= 1 then 'Failed' else 'Passed' end as outcome,null as failurereason,
                                null as evaluatedmetrics ,null as passcnt , null as failcnt,'{year}' as year, '{month}' as month , 
                                '{day}' as day,current_timestamp() as AuditTimestamp
                                from (select source , sum(status)  con_status from 
                (select source,case when outcome = 'Failed' then 1 else 0 end as status  from consolidated_dq_result_df) group by source)"""
    log.info(final_dq_cal_query)
    default_dq_result_df = spark.sql(final_dq_cal_query)
    default_dq_result_df.show()
    
    default_dq_result_df.createOrReplaceTempView("default_dq_result_df")
    final_dq_result_df = spark.sql(f"""select outcome FROM default_dq_result_df """)
    for row in final_dq_result_df.rdd.collect():
        final_dq_status = row['outcome']
    log.info(f"Final DataQuality Result is {final_dq_status} in the workflow id {corelation_id}") 
    
    
    finaldf = consolidated_dq_result_df.union(default_dq_result_df)
    finaldf.show()

    consolidated_dq_result_dyf = DynamicFrame.fromDF(finaldf, glueContext, "consolidated_dq_result_dyf")
    return consolidated_dq_result_dyf , final_dq_status


def failed_processing_generate_dq_overall_status_consolidated(glueContext,spark, corelation_id, source, dq_overall_status_rule_name,
                                                              year, month, day):
    data = [{'corelation_id': corelation_id, 'source': source, 'actual_value': '', 'rule': dq_overall_status_rule_name,
             'outcome': 'Failed', 'failurereason': '', 'evaluatedmetrics': '', 'passcnt': '', 'failcnt': '',
             'year': year, 'month': month,
             'day': day, 'audittimestamp': datetime.now()}]

    log.info(f"Dataset for dqresult in case of failure:{data}")
    # creating a dataframe
    job_failure_dataframe = spark.createDataFrame(data)
    job_failure_dataframe = job_failure_dataframe.select(
        [when(col(c) == '', None).otherwise(col(c)).alias(c) for c in job_failure_dataframe.columns])
    job_failure_dataframe.show()
    job_failure_dyf = DynamicFrame.fromDF(job_failure_dataframe, glueContext, "job_failure_dyf")

    log.info("Generated and loaded the data in consolidated dqresults table")
    return job_failure_dyf