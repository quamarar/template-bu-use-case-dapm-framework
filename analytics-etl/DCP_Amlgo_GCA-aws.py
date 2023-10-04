""""
Things to note:
1. There are 2 athena database variables are defined in order to have the distinction.
Currently all the tables are copied in 1 athena db - dcp_amlgolabs. So both variables are having the same value
At any point in future, these values can be changed if they are directly pointed to the DACL athena databases
    a. curated_sr2_athena_db
    b. curated_sr3_athena_db

"""
import pandas as pd
import awswrangler as wr
from datetime import date,datetime,timedelta
import logging
import sys
from awsglue.utils import getResolvedOptions

log = logging.getLogger(__name__)
logging.basicConfig(format='%(asctime)s :: %(message)s', level=logging.INFO)
logging.info("info")

def get_session(date):
    if pd.isna(date):
        return ''
    else:
        return pd.to_datetime(pd.Series([date])).dt.to_period('Q-MAR').dt.qyear.apply(lambda x: str(x-1) + "-" + str(x))


if __name__ == '__main__':

    try:

        args = getResolvedOptions(sys.argv,
                                  [
                                      "curated_sr3_athena_db",
                                      "curated_sr2_athena_db",
                                      "target_output_path",
                                      "table_name_gcas_chkd",
                                      "table_name_gcas_chkm",
                                      "table_name_sprd_qmmm",
                                      "table_name_ctrr_comp",
                                      "table_name_gcas_dept",
                                      "table_name_gcas_cods"
                                  ])

        curated_sr3_athena_db = args['curated_sr3_athena_db']
        curated_sr2_athena_db = args['curated_sr2_athena_db']
        target_output_path = args['target_output_path']
        table_name_gcas_chkd = args['table_name_gcas_chkd']
        table_name_gcas_chkm = args['table_name_gcas_chkm']
        table_name_sprd_qmmm = args['table_name_sprd_qmmm']
        table_name_ctrr_comp = args['table_name_ctrr_comp']
        table_name_gcas_dept = args['table_name_gcas_dept']
        table_name_gcas_cods = args['table_name_gcas_cods']



        today = date.today()
        year = today.strftime("%Y")
        month = today.strftime("%m")
        day = today.strftime("%d")
        output_path = f"{target_output_path}/year={year}/month={month}/day={day}/"



        query = f""" 
            SELECT    m.chkm_chksht_no,
                         M.CHKM_PLANT PLANT,
                        G.QMMM_DESC||'-'||(select  CASE WHEN M.CHKM_DOM_EXP ='D' then 'DOM' else 'EXP' END)|| (SELECT CASE WHEN M.CHKM_NISSAN='Y' THEN '-N' ELSE '' END) MODEL,
                         m.CHKM_AUDIT_DATE AUDIT_DATE,
                         M.CHKM_VIN_NO VIN_NO,
                         M.CHKM_FUEL_TYPE FUEL,
                         D.CHKD_RESP_DEPT_CODE DPT_CODE,
                         T.DEPT_DESC DEPT,
                         D.CHKD_COMP_CODE LOC_CODE,
                         D.CHKD_COMP_CODE  LOCATION,
                         D.CHKD_DEFECT_CODE DEF_CODE,
                         D.CHKD_DEFECT_CODE DEFECT,
                         D.CHKD_FACTOR_VALUE DMT,
                         M.CHKM_NISSAN NISSA,
                         D.CHKD_REMARKS REMARKS,
                         S.CODS_DESC IQS,
                         D.CHKD_ZONE ZONE,
                         D.CHKD_DEFECT_TYPE NATURE_OF_DEFECT,
                         Z.CTRR_NAME  VENDOR,chkm_color color 
                   FROM {curated_sr2_athena_db}.{table_name_gcas_chkm} M
                  JOIN  {curated_sr2_athena_db}.{table_name_sprd_qmmm} G ON (M.CHKM_BVEH_CODE = G.QMMM_CODE OR M.CHKM_BVEH_CODE = G.QMMM_FAMILY_MODEL_CODE)
                   AND G.QMMM_TYPE =  'O'
                  LEFT OUTER JOIN {curated_sr2_athena_db}.{table_name_gcas_chkd} D ON (M.CHKM_CHKSHT_NO = D.CHKD_CHKSHT_NO)
                  LEFT OUTER JOIN  {curated_sr3_athena_db}.{table_name_ctrr_comp} Z  ON  (Z.CTRR_CODE   = D.CHKD_VEND_CODE)
                  LEFT OUTER JOIN {curated_sr2_athena_db}.{table_name_gcas_dept} T ON  (T.DEPT_CODE = D.CHKD_RESP_DEPT_CODE)
                  LEFT OUTER JOIN {curated_sr2_athena_db}.{table_name_gcas_cods} S ON (S.CODS_CODE = D.CHKD_IQS_CODE)
                  WHERE m.CHKM_AUDIT_DATE BETWEEN CAST('2000-04-01' AS DATE) AND CAST('2099-09-30' AS DATE)
                     and D.CHKD_IQS_CODE!='3' AND S.CODS_TYPE  =  'IQS'
        """

        log.info(f"Query: {query}")

        data = wr.athena.read_sql_query(query, database=curated_sr3_athena_db,ctas_approach=True,keep_files=False)
        log.info("--- Query Executed----")
        log.info("--- Code Pre-processing started----")
        data = data[['model','audit_date','def_code','dmt','nature_of_defect','vendor']]
        data['model'] = data['model'].str[0:3]
        current_time = datetime.now()+timedelta(hours=5, minutes=30)
        current_time = current_time.strftime('%Y-%m-%d %H:%M:%S')
        data['glue_last_updated'] = current_time
        data['session']=data['audit_date'].apply(get_session)
        data['pmonth_year'] = data['audit_date'].dt.strftime('%b-%Y')

        log.info(f"Code Pre-processing Complete and next step is to write the file on path :{output_path}")
        wr.s3.to_parquet(df=data, path=output_path, dataset=True, index=False)
        log.info("--- Processed File saved to S3----")

    except Exception as e:
        log.info(f"Error Occurred :{e}")
        raise e