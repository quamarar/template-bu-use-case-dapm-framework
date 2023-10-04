import awswrangler as wr
import pandas as pd
from datetime import date, datetime, timedelta
import logging
import sys
from awsglue.utils import getResolvedOptions

log = logging.getLogger(__name__)
logging.basicConfig(format='%(asctime)s :: %(message)s', level=logging.INFO)
logging.info("info")


def dateupdate(i):
    if '-' in i[:4]:
        i = i[6:10] + '-' + i[3:5] + '-' + i[0:2] + ' 00:00:00'
    return i


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


def get_city(code):
    try:
        return citystate.get(str(code))[0]
    except:
        return 'Not_Found'


def get_state(code):
    try:
        return citystate.get(str(code))[1]
    except:
        return 'Not_Found'


if __name__ == '__main__':
    try:

        args = getResolvedOptions(sys.argv,
                                  [
                                      "athena_db",
                                      "table_name_ssal_invc",
                                      "table_name_mwar_dmdl",
                                      "target_output_path"
                                  ])
        athena_db = args['athena_db']
        table_name_ssal_invc= args['table_name_ssal_invc']
        table_name_mwar_dmdl= args['table_name_mwar_dmdl']
        target_output_path= args['target_output_path']

        ssal_invc_query = f"select invc_date, invc_pmod_code, invc_spec_code, invc_qty, invc_delr_code from {athena_db}.{table_name_ssal_invc}"
        log.info(f"ssal_invc_query:{ssal_invc_query}")

        mwar_dmdl_query = f"select DELR_CD,CITY,STATE from {athena_db}.{table_name_mwar_dmdl}"
        log.info(f"mwar_dmdl_query:{mwar_dmdl_query}")


        today = date.today()
        year = today.strftime("%Y")
        month = today.strftime("%m")
        day = today.strftime("%d")


        output_path = f"{target_output_path}/year={year}/month={month}/day={day}/"


        df = wr.athena.read_sql_query(ssal_invc_query, database=athena_db, ctas_approach=True)

        log.info("--- Code Pre-processing started----")
        df['invc_date'] = pd.to_datetime(df['invc_date']).dt.strftime('%Y-%m-%d %H:%M:%S.%f')
        df['invc_date'] = df['invc_date'].apply(dateupdate)
        # dmdl=pd.read_csv(path1, delimiter='$', usecols=['DELR_CD','CITY','STATE'])

        dmdl = wr.athena.read_sql_query(mwar_dmdl_query, database=athena_db, ctas_approach=True)
        log.info(f"After reading mwar_dmdl_query , the count is {dmdl.count()}")
        citystate = dmdl.set_index('delr_cd').T.to_dict('list')
        df['City'] = df['invc_delr_code'].apply(get_city)
        df['State'] = df['invc_delr_code'].apply(get_state)
        df = df[df['invc_spec_code'] == 'P74']
        df['Year'] = df['invc_date'].apply(lambda x: int(str(x)[:4]))
        df['Month'] = df['invc_date'].apply(lambda x: int(str(x)[5:7]))
        df['serial_id'] = df[['Month', 'Year']].apply(lambda x: getID(*x), axis=1)
        df['month_year'] = df[['Month', 'Year']].apply(lambda x: get_month_year(*x), axis=1)
        df['model_code'] = df['invc_pmod_code'].apply(lambda x: x[:5])
        df['model_code_full'] = df['invc_pmod_code']
        df['session'] = df[['Month', 'Year']].apply(lambda x: getSession(*x), axis=1)
        df = df.drop(columns=['invc_date', 'invc_spec_code', 'Month', 'Year', 'invc_pmod_code'])
        df = df.groupby(
            ['serial_id', 'month_year', 'model_code', 'model_code_full', 'session', 'City', 'State']).sum().reset_index()
        df = df.rename(columns={'invc_qty': 'count'})[
            ['month_year', 'model_code', 'model_code_full', 'serial_id', 'count', 'session', 'City', 'State']]
        current_time = datetime.now() + timedelta(hours=5, minutes=30)  # current time Indian format
        current_time = current_time.strftime('%Y-%m-%d %H:%M:%S')
        df['glue_last_updated'] = current_time
        df['base_model_code'] = df['model_code_full'].str[0:3]
        df = df[df['model_code_full'].notnull()]
        model_code_del = ['Y', 'Y1W', 'Y2', 'Y50', 'Y50M1', 'Y51', 'Y8', 'Y6K14', 'Y6K14BBC', 'Y6K14BBD', 'YB1', 'YC%',
                          'YCG', 'YE2', 'YL*', 'YLA51C2C', 'YM1', 'YM125', 'YN4', 'YN411', 'YN4B2', 'YN4B2C2B', 'YN4B2C2C',
                          'YN4B2C2D', 'YN4D2C2B', 'YN4D2C2C', 'YN4D2C2D', 'YR4BED', 'YR4CSH', 'YR4BEP', 'YS', 'Y1E19',
                          'Y3A', 'Y3AM', 'Y3AM1', 'Y3AS1', 'Y5A', 'Y5AF9', 'Y5AF9BAA', 'Y5AF9BAB', 'Y5AF9BAC', 'YP7R0',
                          'YXXXX']
        base_code_add = ['Y1W', 'Y50', 'Y51', 'Y6K', 'YB1', 'YCG', 'YE2', 'YLA', 'YM1', 'YN4', 'YR4', 'Y1E', 'Y5A', 'Y3A',
                         'YXX', 'YP7']
        df = df[~df['model_code_full'].isin(model_code_del)]
        df = df[df['model_code_full'].astype(str).str.startswith('Y')]
        dsales_df = df[~df['base_model_code'].isin(base_code_add)]

        log.info("--- Code Pre-processing Complete----")
        wr.s3.to_parquet(df=dsales_df, path=output_path, dataset=True, index=False)
        log.info("--- Processed File saved to S3----")

    except Exception as e:
        log.info(f"Error={e}")
        raise e
