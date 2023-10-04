import pandas as pd
import awswrangler as wr
from datetime import date, datetime, timedelta
import numpy as np
import logging
import sys
from awsglue.utils import getResolvedOptions

#Comment added

log = logging.getLogger(__name__)
logging.basicConfig(format='%(asctime)s :: %(message)s', level=logging.INFO)
logging.info("info")

# code to extract defect response column
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

if __name__ == '__main__':
    try:
        
        today = date.today()
        year = today.strftime("%Y")
        month = today.strftime("%m")
        day = today.strftime("%d")

        args = getResolvedOptions(sys.argv,
                                  [
                                   "curated_sr3_athena_db",
                                   "curated_sr2_athena_db",
                                   "table_name_mmat_vdes",
                                   "table_name_mmat_semm",
                                   "table_name_item_comp",
                                   "table_name_mmat_dfcc",
                                   "table_name_mmat_ctrr_comp",
                                   "table_name_mwar_wrpn",
                                   "table_name_mmat_wc",
                                   "target_output_path"
                                   ])

        
        curated_sr3_athena_db = args['curated_sr3_athena_db']
        curated_sr2_athena_db = args['curated_sr2_athena_db']
        table_name_mmat_vdes = args['table_name_mmat_vdes']
        table_name_mmat_semm = args['table_name_mmat_semm']
        table_name_item_comp = args['table_name_item_comp']
        table_name_mmat_dfcc = args['table_name_mmat_dfcc']
        table_name_mmat_ctrr_comp = args['table_name_mmat_ctrr_comp']
        table_name_mwar_wrpn = args['table_name_mwar_wrpn']
        table_name_mmat_wc = args['table_name_mmat_wc']
        target_output_path = args['target_output_path']

        output_path = f"{target_output_path}/year={year}/month={month}/day={day}/"

        query = f""" 
            with item_comp as
                    (select max(mmat_item_comp.item_desc) as item_desc, mmat_item_comp.item_code 
                    from {curated_sr3_athena_db}.{table_name_item_comp} mmat_item_comp group by mmat_item_comp.item_code )

            ,mmat_ctrr_comp as ( select distinct
                    COALESCE((select COALESCE(max(comp2.CTRR_NAME), Max(comp1.CTRR_NAME)) from {curated_sr3_athena_db}.{table_name_mmat_ctrr_comp} comp1
                    inner join (select distinct CTRR_CODE, CTRR_NAME from {curated_sr3_athena_db}.{table_name_mmat_ctrr_comp} ) comp2
                    On comp1.CTRR_PARENT_VEND_CODE = comp2.CTRR_CODE 
                    where comp1.CTRR_CODE=mv.vdes_ctrr_vend_code), 
                    mv.vdes_ctrr_vend_code) VEND_NAME, mv.vdes_ctrr_vend_code Vendor_Code 
                    from {curated_sr3_athena_db}.{table_name_mmat_vdes} mv)
                    
            select 
            mmat_semm.semm_basic_model as model_code,
          mmat_semm.semm_model_code as full_model_code,
            mmat_vdes.vdes_creation_date as date,
          mmat_vdes.VDES_CTL_NO as control_no,
          mmat_vdes.VDES_LAST_UPDATED_ON as last_update_on,
          mmat_vdes.VDES_STATUS as line_status,
            mmat_vdes.vdes_item_part_no as part_number,
            item_comp.item_desc as part_name,
            mmat_vdes.vdes_ctrr_vend_code as vendor_code,
            --mmat_ctrr_comp.CTRR_PARENT_VEND_CODE as Perent_Vendor_Code,
            mmat_ctrr_comp.vend_name as vendor_name,
            mmat_vdes.vdes_prob_desc as observation,
            mmat_vdes.vdes_catg as defect_category,
            mmat_vdes.vdes_repeat_flag as rep_defect,
            mmat_dfcc.dfcc_desc as causes_category,
            wp.WRPN_DESC as plant,
            mmat_wc.wc_dept_code,
            mmat_wc.wc_name,
            mmat_wc.wc_rec_no
            from {curated_sr3_athena_db}.{table_name_mmat_vdes} mmat_vdes  
            left join {curated_sr3_athena_db}.{table_name_mmat_semm} mmat_semm on mmat_semm.semm_model_code=mmat_vdes.vdes_semm_model_code
            left join  item_comp on item_comp.item_code=mmat_vdes.vdes_item_part_no
            left join {curated_sr3_athena_db}.{table_name_mmat_dfcc} mmat_dfcc on cast(mmat_vdes.vdes_cause_code as integer)= cast(mmat_dfcc.dfcc_code as integer)
            left join mmat_ctrr_comp on mmat_ctrr_comp.vendor_code = mmat_vdes.vdes_ctrr_vend_code
            left join {curated_sr2_athena_db}.{table_name_mwar_wrpn} wp on wp.WRPN_CODE = mmat_vdes.vdes_psln_plant
            left join {curated_sr3_athena_db}.{table_name_mmat_wc} mmat_wc ON mmat_vdes.vdes_section = mmat_wc.wc_rec_no

            """
        log.info(f"Query: {query}")

        data = wr.athena.read_sql_query(query, database=curated_sr3_athena_db , ctas_approach=True, keep_files=False)
        log.info(f"the first count: {data.count()}")
        log.info("--- Query Executed----")
        log.info("--- Code Pre-processing started----")
        log.info(f"the count is {data.count()}")
        data['pmonth_year'.lower()] = data['date'].dt.strftime('%b-%Y')
        data['session'] = data['date'].dt.to_period('Q-MAR').dt.qyear.apply(lambda x: str(x - 1) + "-" + str(x))
        data['model_code'] = data['model_code'].apply(lambda x: np.nan if pd.isna(x) or x.strip() == '' else x)
        data['model_code'] = data['model_code'].fillna(data['full_model_code'].str[:5])
        data = data.drop(['full_model_code'], axis=1)
        data['month'] = data['date'].dt.month_name()


        temp = data[data.duplicated(['date', 'control_no'], keep=False)]
        log.info(f" line 95 temp dupe check count: {data.count()}")
        newdata = pd.concat([data, temp]).drop_duplicates(keep=False)
        log.info(f" line 98 newdata dupe check count: {data.count()}")
        temp = temp[~temp['last_update_on'].isnull()]
        log.info(f" after dupe remove  in temp count: {data.count()}")
        data = pd.concat([newdata, temp])
        log.info(f"after  dupe delete count: {data.count()}")
        data = data[~data['line_status'].isin(['X'])]
        log.info(f"after   X delete count: {data.count()}")
        current_time = datetime.now() + timedelta(hours=5, minutes=30)
        data['glue_last_updated'] = current_time.strftime('%Y-%m-%d %H:%M:%S')
        data['defect_resp'] = data['wc_dept_code'].apply(lambda x: get_dfect_resp(x))
        data['base_model_code'] = data['model_code'].str[0:3]
        data = data[data['model_code'].notnull()]
        model_code_del = ['Y', 'Y1W', 'Y2', 'Y50', 'Y50M1', 'Y51', 'Y8', 'Y6K14', 'Y6K14BBC', 'Y6K14BBD', 'YB1', 'YC%',
                          'YCG', 'YE2', 'YL*', 'YLA51C2C', 'YM1', 'YM125', 'YN4', 'YN411', 'YN4B2', 'YN4B2C2B', 'YN4B2C2C',
                          'YN4B2C2D', 'YN4D2C2B', 'YN4D2C2C', 'YN4D2C2D', 'YR4BED', 'YR4CSH', 'YR4BEP', 'YS', 'Y1E19',
                          'Y3A', 'Y3AM', 'Y3AM1', 'Y3AS1', 'Y5A', 'Y5AF9', 'Y5AF9BAA', 'Y5AF9BAB', 'Y5AF9BAC', 'YP7R0',
                          'YXXXX']
        base_code_add = ['Y1W', 'Y50', 'Y51', 'Y6K', 'YB1', 'YCG', 'YE2', 'YLA', 'YM1', 'YN4', 'YR4', 'Y1E', 'Y5A', 'Y3A',
                         'YXX', 'YP7']
        data = data[~data['model_code'].isin(model_code_del)]
        log.info(f"after   model_code delete count: {data.count()}")
        data = data[data['model_code'].astype(str).str.startswith('Y')]
        data = data[~data['base_model_code'].isin(base_code_add)]
        log.info(f"after  base model_code delete count: {data.count()}")
        data['location'] = data['control_no'].str[:4]
        data['location'] = data['location'].apply(lambda x: x.replace('-', ''))
        data['location'] = data['location'].replace(['MUL', 'MULG', 'MULM', 'SPIL', 'TMP'], 'MSIL')
        data = data[
            ['model_code', 'part_number', 'part_name', 'vendor_code', 'vendor_name', 'observation', 'defect_category',
             'rep_defect', 'causes_category', 'plant', 'pmonth_year', 'session', 'month', 'glue_last_updated',
             'defect_resp', 'wc_dept_code', 'wc_name', 'wc_rec_no', 'base_model_code', 'date', 'control_no',
             'last_update_on', 'line_status', 'location']]
             
        log.info("--- Code Pre-processing Complete----")
        wr.s3.to_parquet(df=data, path=output_path, dataset=True)
        log.info("--- Processed File saved to S3----")
    except Exception as e:
        log.info(f"Error Occurred :{e}")
        raise e 

