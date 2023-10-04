""""
Things to note:
1. There are 4  athena database variables are defined in order to have the distinction.
Currently all the tables are copied in 1 athena db - dcp_amlgolabs. So all these 4 variables are having the same value
At any point in future, these values can be changed if they are directly pointed to the DACL athena databases
    a. curated_sr2_athena_db
    b. curated_sr3_athena_db
    c. datalake_transformed_athena_db
    d. dcp_athena_db
"""

from datetime import date, datetime, timedelta
import awswrangler as wr
import boto3, time
import pandas as pd
import DCPArchivefiles
import logging
import sys
from awsglue.utils import getResolvedOptions

log = logging.getLogger(__name__)
logging.basicConfig(format='%(asctime)s :: %(message)s', level=logging.INFO)
logging.info("info")


def formatdate(df, cols, count):
    try:
        for column in cols:
            df[column] = pd.to_datetime(df[column]).dt.strftime('%Y-%m-%d %H:%M:%S.%f')
    except Exception as e:
        log.info('Error:{e}')
    else:
        log.info(f'Table{count} timestamp updated')


def find_match(x, y, default_dict):
    try:
        return regex.search(f'(?<=\s){x}\w+', y).group()
    except:
        return default_dict[x]


if __name__ == '__main__':
    try:
    
        args = getResolvedOptions(sys.argv,
                                  [
                                      "target_output_path",
                                      "partnamepath",
                                      "filepath_platform_model_wise",
                                      "curated_sr2_athena_db",
                                      "curated_sr3_athena_db",
                                      "datalake_transformed_athena_db",
                                      "dcp_athena_db",
                                      "table_name_mmat_ctrr_comp",
                                      "table_name_shopfloor_mapping",
                                      "table_name_mmat_item_comp",
                                      "table_name_mwar_wrpn",
                                      "table_name_mwar_modl",
                                      "table_name_mwar_clam",
                                      "table_name_mwar_clm2",
                                      "table_name_mwar_vdtl",
                                      "table_name_mwar_dmdl",
                                      "table_name_mwar_tt_month",
                                      "table_name_mwar_mile",
                                      "table_name_mwar_ptyp",
                                      "table_name_mwar_judg"
    
                                  ])
    
        target_output_path = args['target_output_path']
        partnamepath = args['partnamepath']
        filepath_platform_model_wise = args['filepath_platform_model_wise']
        curated_sr2_athena_db = args['curated_sr2_athena_db']
        curated_sr3_athena_db = args['curated_sr3_athena_db']
        datalake_transformed_athena_db = args['datalake_transformed_athena_db']
        dcp_athena_db = args['dcp_athena_db']
        table_name_mmat_ctrr_comp = args['table_name_mmat_ctrr_comp']
        table_name_shopfloor_mapping = args['table_name_shopfloor_mapping']
        table_name_mmat_item_comp = args['table_name_mmat_item_comp']
    
        table_name_mwar_wrpn = args['table_name_mwar_wrpn']
        table_name_mwar_modl = args['table_name_mwar_modl']
        table_name_mwar_clam = args['table_name_mwar_clam']
        table_name_mwar_clm2 = args['table_name_mwar_clm2']
        table_name_mwar_vdtl = args['table_name_mwar_vdtl']
        table_name_mwar_dmdl = args['table_name_mwar_dmdl']
        table_name_mwar_tt_month = args['table_name_mwar_tt_month']
        table_name_mwar_mile = args['table_name_mwar_mile']
        table_name_mwar_ptyp = args['table_name_mwar_ptyp']
        table_name_mwar_judg = args['table_name_mwar_judg']
    
        today = date.today()
        year = today.strftime("%Y")
        month = today.strftime("%m")
        day = today.strftime("%d")
        output_path = f"{target_output_path}/year={year}/month={month}/day={day}/"
    
        query = f"""select distinct
                CL.CLAM_ISSUE_NO,
                PTP.PTYP_PART_TYPE,
                CL.CLAM_ODOMETER,
                cl.clam_shop_floor,
                CL.CLAM_CLAM_TYPE_DMS,
                CLAM_REPAIR_DT,
                CLAM_SRATR_DATE,
                MNT.FYEAR,
                PTP.PTYP_PART_DESC,
                MNT.MONTH,
                DM.CITY,
                DM.STATE,
                COALESCE((select COALESCE(max(comp2.CTRR_NAME), Max(comp1.CTRR_NAME)) 
                from {curated_sr3_athena_db}.{table_name_mmat_ctrr_comp} comp1 
                inner join (select distinct CTRR_CODE, CTRR_NAME from {curated_sr3_athena_db}.{table_name_mmat_ctrr_comp} ) comp2 
                On comp1.CTRR_PARENT_VEND_CODE = comp2.CTRR_CODE where comp1.CTRR_CODE=CL.CLAM_VEND_CD),CLAM_VEND_CD) "VENDOR_NAME",
                cl.clam_vend_cd,
                MI.MILE_DESC,
                COALESCE(CL.CLAM_SHOP_FLOOR, 'X') "SHOP_FLR_CD",
                (SELECT SM.description from {dcp_athena_db}.{table_name_shopfloor_mapping} SM 
                where sm.code=(COALESCe(CL.CLAM_SHOP_FLOOR, 'X'))) "defect_resp",
                CLM2_CASUAL_PART,
                C2.CLM2_PART_NO,
                DM.REGION,
                COALESCE((SELECT ITEM_DESC FROM {curated_sr3_athena_db}.{table_name_mmat_item_comp} MMAT_ITEM_COMP 
                WHERE ITEM_CODE = CLM2_PART_NO AND ITEM_COMP_CODE = '01'),CLM2_PART_NO) "CAUSAL_PART_NAME",
                (SELECT RP.WRPN_DESC FROM {curated_sr2_athena_db}.{table_name_mwar_wrpn} RP WHERE RP.WRPN_CODE = CL.CLAM_PLANT_NO) "PLANT",
                MNT_FCOK.MONTH "FCOK_MONTH",
                MNT_FCOK.FYEAR "FCOK_YEAR",
                CL.CLAM_FCOK_DATE,
                CL.CLAM_PROCESS_DT,
                (SELECT DISTINCT MODL_DESC FROM {curated_sr2_athena_db}.{table_name_mwar_modl} MD WHERE MD.MODL_WTY_CD=CL.CLAM_MODEL_CD AND MD.MODL_MKTG_CD = CL.CLAM_BVEH_CODE ) "MODEL_DSEC",
                (SELECT DISTINCT MODL_MKTG_CD FROM {curated_sr2_athena_db}.{table_name_mwar_modl} MD WHERE MD.MODL_WTY_CD=CL.CLAM_MODEL_CD AND MD.MODL_MKTG_CD = CL.CLAM_BVEH_CODE ) "MODL_SERVICE_CODE",
                VD.VDTL_FUEL,
                VD.VDTL_TM_TYPE,
                VD.VDTL_ENGI_PREFIX,
                VD.VDTL_PMOD_DESC,
                CL.CLAM_CREDIT_AMT,
                GREATEST(date_diff('day',CAST(clam_register_dt AS DATE),CAST(clam_repair_dt AS DATE)),0) "days_used"
                FROM {curated_sr2_athena_db}.{table_name_mwar_clam} CL
                INNER JOIN {curated_sr2_athena_db}.{table_name_mwar_clm2} C2
                ON CL.CLAM_DEIND = C2.CLM2_DEIND
                AND CL.CLAM_DELR_CD = C2.CLM2_DELR_CD
                AND CL.CLAM_FOR_CD = C2.CLM2_FOR_CD
                AND CL.CLAM_OUTLET_CD = C2.CLM2_OUTLET_CD
                AND CL.CLAM_DUP_SL_NO = C2.CLM2_DUP_SL_NO
                AND CL.CLAM_ISSUE_NO = C2.CLM2_ISSUE_NO
                AND 1= CASE WHEN (SELECT COUNT(1)
                                      FROM {curated_sr2_athena_db}.{table_name_mwar_clm2} C3
                                     WHERE C3.CLM2_DELR_CD = CL.CLAM_DELR_CD
                                       AND C3.CLM2_FOR_CD = CL.CLAM_FOR_CD
                                       AND C3.CLM2_OUTLET_CD = CL.CLAM_OUTLET_CD
                                       AND C3.CLM2_ISSUE_NO = CL.CLAM_ISSUE_NO) > 1
                                              AND    C2.CLM2_CASUAL_PART = 'X'  THEN 1
    
                                             WHEN (SELECT COUNT(1)
                                      FROM {curated_sr2_athena_db}.{table_name_mwar_clm2} C3
                                     WHERE C3.CLM2_DELR_CD = CL.CLAM_DELR_CD
                                       AND C3.CLM2_FOR_CD = CL.CLAM_FOR_CD
                                       AND C3.CLM2_OUTLET_CD = CL.CLAM_OUTLET_CD
                                       AND C3.CLM2_ISSUE_NO = CL.CLAM_ISSUE_NO) = 1
                                              AND     C2.CLM2_CASUAL_PART in ('X','L')   THEN 1
                END
                LEFT JOIN (SELECT DISTINCT * FROM {curated_sr2_athena_db}.{table_name_mwar_vdtl}) VD
                ON CL.CLAM_VIN_NO = VD.VDTL_VIN
                INNER JOIN (SELECT DISTINCT * FROM {datalake_transformed_athena_db}.{table_name_mwar_dmdl}) DM 
                ON CL.CLAM_DELR_CD = DM.DELR_CD
                AND CL.CLAM_FOR_CD = DM.FOR_CD
                AND CL.CLAM_OUTLET_CD = DM.OUTLET_CD
                INNER JOIN (SELECT DISTINCT * FROM {curated_sr2_athena_db}.{table_name_mwar_tt_month}) MNT_FCOK
                ON 1=1 AND CL.CLAM_FCOK_DATE BETWEEN MNT_FCOK.CALN_START_DATE AND MNT_FCOK.CALN_END_DATE
                INNER join (SELECT DISTINCT * FROM {curated_sr2_athena_db}.{table_name_mwar_tt_month}) MNT
                ON 1=1 AND CL.CLAM_PROCESS_DT BETWEEN MNT.START_DATE AND MNT.END_DATE
                Inner Join (SELECT DISTINCT * FROM {curated_sr2_athena_db}.{table_name_mwar_mile}) MI
                ON COALESCE(CL.CLAM_ODOMETER,-1) BETWEEN MI.MILE_START_KM AND MI.MILE_END_KM
                Inner Join {curated_sr2_athena_db}.{table_name_mwar_ptyp} PTP
                ON COALESCE(C2.CLM2_PART_TYPE, 'X') = PTP.PTYP_PART_TYPE
                inner join {curated_sr2_athena_db}.{table_name_mwar_judg} JU ON
                CL.CLAM_JUDGE_CD = JU.JUDG_CD
                where CL.CLAM_PROCESS_DT between CAST('2016-04-01' AS DATE) AND CAST('2035-03-31' AS DATE)
                and CL.CLAM_DEIND='D'	
                and CL.clam_judge_cd='F00'		
                and CL.clam_war_party_type='DLR' 	
                AND trim(CL.CLAM_DELR_CD) <> 'CTMT'	
                AND trim(CL.CLAM_DELR_CD) IS NOT NULL  	
                AND trim(CL.CLAM_VEND_CD) <> 'I400'  
                AND trim(CL.CLAM_VEND_CD) IS NOT NULL 
                AND CAST(CL.CLAM_CREDIT_AMT AS DOUBLE) > 0 """
    
        log.info(f"Query:{query}")
        master_files = ['MWAR_CLAM', 'MWAR_CLM2', 'MWAR_VDTL']
    
        tabledate = {
            'MWAR_CLAM': ['clam_lot_iss_dt', 'clam_register_dt', 'clam_repair_dt', 'clam_process_dt', 'clam_system_date',
                          'clam_mul_inv_date', 'clam_fcok_date', 'clam_sratr_date'],
            'MWAR_VDTL': ['vdtl_dispatched_date']}
    
        data = wr.athena.read_sql_query(query, database=dcp_athena_db, ctas_approach=True)
        log.info("Dataframe captured after reading data from the athena query")
    
        df = pd.read_csv(partnamepath)
        log.info(f"Dataframe captured after reading file from {partnamepath}")
    
        default_dict = {
            'D13': 'D13A',
            'D16': 'D16',
            'E08': 'E08',
            'E15': 'E15A',
            'F8B': 'F8B',
            'F8D': 'F8D',
            'F10': 'F10',
            'G12': 'G12B',
            'G13': 'G13',
            'K10': 'K10B',
            'K12': 'K12M',
            'K14': 'K14B',
            'K15': 'K15B', 'G16': 'G16',
            'M15': 'M15A', 'G10': 'G10',
            'M16': 'M16', 'XXX': 'XXX'}
    
        log.info("--- Code Pre-processing started----")
        platform = pd.read_excel(filepath_platform_model_wise)
        log.info(f"Dataframe captured after reading file from path : {filepath_platform_model_wise}")
    
        platform_dict = dict(zip(platform['Model'], platform['Platform']))
        partmap = dict(zip(df['part_number'], df['part_name']))
        data["month"] = data["month"].apply(lambda x: x[:4] + '20' + x[4:])
        data['fcok_month'] = data['fcok_month'].apply(lambda x: x[:4] + '20' + x[4:])
        data['base_model_code'] = data['modl_service_code'].apply(lambda x: x[:3])
        data['platform'] = data['base_model_code'].map(platform_dict)
        data['vdtl_engi_prefix'].fillna('XXX', inplace=True)
        data['engine_type'] = data[['vdtl_engi_prefix', 'vdtl_pmod_desc']].apply(
            lambda x: find_match(x[0], x[1], default_dict), axis=1)
        data['causal_part_name'] = data['clm2_part_no'].apply(lambda x: partmap.get(x, ''))
        current_time = datetime.now() + timedelta(hours=5, minutes=30)  # current time Indian format
        data['glue_last_updated'] = current_time.strftime('%Y-%m-%d %H:%M:%S')
        data['index'] = list(range(len(data)))
        data['uid'] = data['index'].apply(lambda x: str(x)) + '-' + data['glue_last_updated'].apply(lambda x: str(x))
        data.drop('index', axis=1, inplace=True)
        log.info("--- Code Pre-processing Complete----")
    
        wr.s3.to_parquet(df=data, path=output_path, dataset=True, index=False)
        log.info("--- Processed File saved to S3----")
    except Exception as e:
        log.info(f"Error Occurred :{e}")
        raise e