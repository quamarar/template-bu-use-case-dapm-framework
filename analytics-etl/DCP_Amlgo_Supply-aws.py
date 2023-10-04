import awswrangler as wr
from datetime import date, datetime, timedelta
import logging
import sys
from awsglue.utils import getResolvedOptions

log = logging.getLogger(__name__)
logging.basicConfig(format='%(asctime)s :: %(message)s', level=logging.INFO)
logging.info("info")


def getSession(month, year):
    x, y = year, year + 1
    if month <= 3: x, y = x - 1, y - 1
    return str(x) + "-" + str(y)


def getID(month, year):
    return (year - 1989) * 12 + month - 2


def getFullYear(month_year):
    year = month_year[-2:]
    if int(year) < 50:
        return month_year[:-2] + "20" + year
    else:
        return month_year[:-2] + "19" + year


def getMonth(month_int): return ['JAN', 'FEB', 'MAR', 'APR', 'MAY', 'JUN', 'JUL', 'AUG', 'SEP', 'OCT', 'NOV', 'DEC'][
    month_int - 1]


def getMonthID(month): return ['JAN', 'FEB', 'MAR', 'APR', 'MAY', 'JUN', 'JUL', 'AUG', 'SEP', 'OCT', 'NOV',
                               'DEC'].index(month) + 1


def get_month_year(month, year):
    return getMonth(int(month)) + "-" + str(year)



# curated_sr3_athena_db = 'dcp_amlgolabs'
# table_name_ctrr_comp = 'mat_de_mmat_ctrr_comp'
# table_name_mmat_venp = 'mat_de_mmat_venp'
# target_output_path = 's3://dcp-dev-apsouth1-analytics/ProcessedData/Supply'

if __name__ == '__main__':
    try:

        today = date.today()
        year = today.strftime("%Y")
        month = today.strftime("%m")
        day = today.strftime("%d")

        args = getResolvedOptions(sys.argv,
                                  [ 
                                   "curated_sr3_athena_db",
                                   "table_name_ctrr_comp",
                                   "table_name_mmat_venp",
                                   "target_output_path"
                                   ])

        
        curated_sr3_athena_db = args['curated_sr3_athena_db']
        table_name_ctrr_comp = args['table_name_ctrr_comp']
        table_name_mmat_venp = args['table_name_mmat_venp']
        target_output_path = args['target_output_path']

        output_path = f"{target_output_path}/year={year}/month={month}/day={day}/"

        query = f""" 
                 select distinct
                COALESCE((select COALESCE(max(comp2.CTRR_NAME), Max(comp1.CTRR_NAME)) 
                from {curated_sr3_athena_db}.{table_name_ctrr_comp} comp1
                inner join (select distinct CTRR_CODE, CTRR_NAME from {curated_sr3_athena_db}.{table_name_ctrr_comp} ) comp2
                On comp1.CTRR_PARENT_VEND_CODE = comp2.CTRR_CODE
                where comp1.CTRR_CODE=VP.venp_vend_code),VP.venp_vend_code) VEND_NAME,
                VP.venp_vend_code     Vendor_Code,
                vp.venp_root_part,vp.venp_item_code,  vp.venp_month,vp.venp_tot_recd_qty
                from {curated_sr3_athena_db}.{table_name_mmat_venp} vp"""
    
        log.info(f"Query : {query}")
        supply_df = wr.athena.read_sql_query(query, database=curated_sr3_athena_db, ctas_approach=True, keep_files=False)
        log.info("--- Query Executed----")
        log.info("--- Code Pre-processing started----")
        if 'vend_name' in supply_df.columns:
            supply_df = supply_df.rename(columns={'vend_name': 'vendor_name'})
        supply_df = supply_df.rename(
            columns={'VENP_VEND_CODE': 'vendor_code', 'venp_item_code': 'part_number', 'venp_root_part': 'root_part_number',
                     'venp_tot_recd_qty': 'count'})
        log.info(supply_df.columns)
        supply_df = supply_df[supply_df['count'].notnull()]
        supply_df['Year'] = supply_df['venp_month'].apply(lambda x: int(str(x)[:4]))
        supply_df['Month'] = supply_df['venp_month'].apply(lambda x: int(str(x)[-2:]))
        supply_df['month_year'] = supply_df[['Month', 'Year']].apply(lambda x: get_month_year(*x), axis=1)
        supply_df['serial_id'] = supply_df[['Month', 'Year']].apply(lambda x: getID(*x), axis=1)
        supply_df = supply_df[
            ['part_number', 'root_part_number', 'count', 'vendor_name', 'vendor_code', 'month_year', 'serial_id']]
        current_time = datetime.now() + timedelta(hours=5, minutes=30)
        supply_df['glue_last_updated'] = current_time.strftime('%Y-%m-%d %H:%M:%S')
        log.info("--- Code Pre-processing Complete----")

        # wr.s3.to_parquet(df=supply_df, path=output_path, dataset=True, index=False)
        # log.info("--- Processed File saved to S3----")
        
        # output_path = f"{target_output_path}/csvversion/year={year}/month={month}/day={day}/"
        wr.s3.to_csv(df=supply_df, path=output_path,dataset=True ,index=False)
        log.info("--- Processed csv File saved to S3----")
    except Exception as e:
        log.info(f"Error Occurred :{e}")
        raise e