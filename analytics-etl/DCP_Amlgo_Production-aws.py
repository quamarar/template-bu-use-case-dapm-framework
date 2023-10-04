import awswrangler as wr
import pandas as pd
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


def filter_func(x):
    if len(x) > 3 and x[1] == 'A' and x[-3:] == '500':
        return x


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


def reduce_nan_to_0(x, y):
    if x: return 0
    return y


def msilfilter(x):
    msil, smg = [], []
    for location in x:
        if location[0].isalpha():
            smg.append(location)
        else:
            msil.append(location)
    return set(msil), set(smg)


if __name__ == '__main__':
    try:

        args = getResolvedOptions(sys.argv,
                                  [
                                      "target_output_path",
                                      "dcp_athena_db",
                                      "tablename_sprd_pday"])

        tablename_sprd_pday = args['tablename_sprd_pday']
        dcp_athena_db = args['dcp_athena_db']
        target_output_path = args['target_output_path']


        today = date.today()
        year = today.strftime("%Y")
        month = today.strftime("%m")
        day = today.strftime("%d")
        output_path = f"{target_output_path}/year={year}/month={month}/day={day}/"

        log.info("---Athena query reading started----")

        sprd_pday_query = f"""select pday_date,pday_ctrlpt_category,pday_ctrlpt_number,pday_pmodel_code,pday_spec_code,pday_colour
                                    ,pday_quantity_a,pday_quantity_b,pday_plan_maximum,pday_prod_type,pday_vehm_selection_code,pday_body_colo_code
                                    ,pday_supp_colo_code from {dcp_athena_db}.{tablename_sprd_pday}"""

        log.info(f"Query: {sprd_pday_query}")

        prod = wr.athena.read_sql_query(sprd_pday_query, database=dcp_athena_db, ctas_approach=True)
        log.info(f"the file count is {prod.count()}")
        log.info('Pandas dataframe captured after reading data from athena table ')


        log.info("--- Code Pre-processing started----")

        # ABOK_list = ['1A3500', '1A5500', '1AB500', '1A1500', '1A4500', '2A1500', '3A1500', '6A1500','7A1500', '8A1500', 'AA1500', 'BA1500','CA1500']
        ABOK_list = set(list(filter(filter_func, prod['pday_ctrlpt_number'])))
        prod = prod[prod['pday_ctrlpt_number'].apply(lambda x: x in ABOK_list)]
        prod['pday_date'] = pd.to_datetime(prod['pday_date']).dt.strftime('%Y-%m-%d %H:%M:%S.%f')
        prod['Year'] = prod['pday_date'].apply(lambda x: int(str(x)[:4]))
        prod['Month'] = prod['pday_date'].apply(lambda x: int(str(x)[5:7]))
        prod['month_year'] = prod[['Month', 'Year']].apply(lambda x: get_month_year(*x), axis=1)
        prod = prod.rename(columns={'pday_pmodel_code': 'model_code'})
        prod['model_code'] = prod['model_code'].apply(lambda x: str(x)[:5])
        prod['is_a_null'] = prod['pday_quantity_a'].isnull()
        prod['is_b_null'] = prod['pday_quantity_b'].isnull()
        prod['pday_quantity_a'] = prod[['is_a_null', 'pday_quantity_a']].apply(lambda x: reduce_nan_to_0(*x), axis=1)
        prod['pday_quantity_b'] = prod[['is_b_null', 'pday_quantity_b']].apply(lambda x: reduce_nan_to_0(*x), axis=1)
        prod['count'] = prod['pday_quantity_a'] + prod['pday_quantity_b']
        prod['serial_id'] = prod[['Month', 'Year']].apply(lambda x: getID(*x), axis=1)
        prod['session'] = prod[['Month', 'Year']].apply(lambda x: getSession(*x), axis=1)
        msil_list, smg_list = msilfilter(
            ABOK_list)  # ['1A1500', '1A3500', '1A4500', '1A5500', '1AB500', '2A1500', '3A1500', '6A1500', '7A1500', '8A1500']
        # smg_list = ['AA1500', 'BA1500', 'CA1500']
        prod['location'] = prod['pday_ctrlpt_number'].apply(
            lambda x: 'MSIL' if x in msil_list else 'SMG' if x in smg_list else None)
        final = prod[
            ['month_year', 'model_code', 'count', 'serial_id', 'session', 'location', 'pday_ctrlpt_number']].groupby(
            ['month_year', 'model_code', 'serial_id', 'session', 'location', 'pday_ctrlpt_number']).sum().reset_index()
        current_time = datetime.now() + timedelta(hours=5, minutes=30)  # current time Indian format
        current_time = current_time.strftime('%Y-%m-%d %H:%M:%S')
        final['glue_last_updated'] = current_time
        final['base_model_code'] = final['model_code'].str[0:3]
        # remove null values from model code/change the column name properly
        final = final[final['model_code'].notnull()]
        model_code_del = ['Y', 'Y1W', 'Y2', 'Y50', 'Y50M1', 'Y51', 'Y8', 'Y6K14', 'Y6K14BBC', 'Y6K14BBD', 'YB1', 'YC%',
                          'YCG', 'YE2', 'YL*', 'YLA51C2C', 'YM1', 'YM125', 'YN4', 'YN411', 'YN4B2', 'YN4B2C2B', 'YN4B2C2C',
                          'YN4B2C2D', 'YN4D2C2B', 'YN4D2C2C', 'YN4D2C2D', 'YR4BED', 'YR4CSH', 'YR4BEP', 'YS', 'Y1E19',
                          'Y3A', 'Y3AM', 'Y3AM1', 'Y3AS1', 'Y5A', 'Y5AF9', 'Y5AF9BAA', 'Y5AF9BAB', 'Y5AF9BAC', 'YP7R0',
                          'YXXXX']
        base_code_add = ['Y1W', 'Y50', 'Y51', 'Y6K', 'YB1', 'YCG', 'YE2', 'YLA', 'YM1', 'YN4', 'YR4', 'Y1E', 'Y5A', 'Y3A',
                         'YXX', 'YP7']
        final = final[~final['model_code'].isin(model_code_del)]
        final = final[final['model_code'].astype(str).str.startswith('Y')]
        final = final[~final['base_model_code'].isin(base_code_add)]
        final = final[['month_year', 'model_code', 'serial_id', 'session', 'count', 'glue_last_updated', 'base_model_code',
                       'location', 'pday_ctrlpt_number']]
        final['month_year'] = pd.to_datetime(final['month_year'])
        final['serial_id'] = final['serial_id'].astype(int)
        final['glue_last_updated'] = pd.to_datetime(final['glue_last_updated'])

        log.info(f"Code Pre-processing Complete and next step is to write the dataframe on path : {output_path}")
        wr.s3.to_parquet(df=final, path=output_path, dataset=True, index=False)
        log.info("--- Processed File saved to S3----")

    except Exception as e:
        log.info(f"The Error={e}")
        raise e


