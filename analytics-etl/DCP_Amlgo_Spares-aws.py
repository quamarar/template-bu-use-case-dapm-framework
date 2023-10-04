import awswrangler as wr
import pandas as pd
from datetime import date, datetime, timedelta
import logging
import sys
from awsglue.utils import getResolvedOptions

log = logging.getLogger(__name__)
logging.basicConfig(format='%(asctime)s :: %(message)s', level=logging.INFO)
logging.info("info")


if __name__ == '__main__':
    try:

        args = getResolvedOptions(sys.argv,
                                  [
                                      "target_output_path",
                                      "curated_sr3_athena_db",
                                      "table_name_mspr_invd",
                                      "table_name_mspr_invo",
                                      "table_name_mspr_cust",
                                      "table_name_mspr_addr",
                                      "table_name_mmat_item_comp"
                                  ])

        target_output_path = args['target_output_path']
        curated_sr3_athena_db = args['curated_sr3_athena_db']
        table_name_mspr_invd = args['table_name_mspr_invd']
        table_name_mspr_invo = args['table_name_mspr_invo']
        table_name_mspr_cust = args['table_name_mspr_cust']
        table_name_mspr_addr = args['table_name_mspr_addr']
        table_name_mmat_item_comp = args['table_name_mmat_item_comp']

        today = date.today()
        year = today.strftime("%Y")
        month = today.strftime("%m")
        day = today.strftime("%d")
        output_path = f"{target_output_path}/year={year}/month={month}/day={day}/"

        master_files = ['MSPR_INVD', 'MSPR_INVO']
        
        dates = ['2015-04-01', '2016-03-31', '2016-04-01', '2017-03-31', '2017-04-01', '2018-03-31', '2018-04-01',
                 '2019-03-31',
                 '2020-04-01', '2021-03-31' , '2022-04-01','2023-03-31']
        count = 0
        for i in range(0, len(dates) - 1, 2):
            query = f"""select d.invd_item_code,
            (select max(item_desc) from  {curated_sr3_athena_db}.{table_name_mmat_item_comp}    
            WHERE item_code = d.invd_item_code and item_comp_Code = o.invo_comp_code) item_name,
               invo_date invoice_date,
               COALESCE(invd_qty, 0) sale_qty,
               r.addr_city City,
               r.ADDR_IN_STAT_ID State
            from {curated_sr3_athena_db}.{table_name_mspr_invd} d, 
            {curated_sr3_athena_db}.{table_name_mspr_invo} o, 
            {curated_sr3_athena_db}.{table_name_mspr_cust}, 
            {curated_sr3_athena_db}.{table_name_mspr_addr} r
            where d.INVD_CUST_CCAT_ID = o.invo_cust_ccat_id
             and d.INVD_CUST_ID = o.invo_cust_id
            and invo_cust_ccat_id = cust_ccat_id
            and invo_cust_id = cust_id
            and cust_ccat_id = r.addr_cust_ccat_id
            and cust_id = r.addr_cust_id
            and d.INVD_INVO_NO = o.invo_no 
            and o.invo_date between CAST('{dates[i]}' AS DATE) AND CAST('{dates[i + 1]}' AS DATE)
            """
            log.info(query)
            df = wr.athena.read_sql_query(query, database=curated_sr3_athena_db, ctas_approach=True)
            len_df = len(df)
            
            log.info(f"Query Executed with resultset length : {len_df}")
            
            if len_df >0 : # Added this condition since the sample data was not having data between mentioned date
                log.info("--- Code Pre-processing started----")
                current_time = datetime.now() + timedelta(hours=5, minutes=30)
                current_time = current_time.strftime('%Y-%m-%d %H:%M:%S')
                df['glue_last_updated'] = current_time
                df['session'] = pd.to_datetime(df['invoice_date']).dt.to_period('Q-MAR').dt.qyear.apply(
                    lambda x: str(x - 1) + "-" + str(x))
                log.info("--- Code Pre-processing Complete----")
    
                wr.s3.to_parquet(df=df, path=output_path, dataset=True, index=False)
                log.info("--- Processed File saved to S3----")
    except Exception as e:
        log.info(f"Error Occurred :{e}")
        raise e