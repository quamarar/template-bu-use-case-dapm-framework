"""
Note:
    1. As per the discussion with Mahendra San , output file of this job is not changed to hive style partition
    due to business reason as everytime all the files are getting processed
    Jira - https://marutide.atlassian.net/browse/M1-97
    2. Refactoring only includes paramterizing the input and hive style partition on the archiving path
"""

# import pandas as pd
# import numpy as np
# from collections import Counter
# from tqdm import tqdm
# tqdm.pandas()

import awswrangler as wr, re, boto3, math, DCPArchivefiles, DCPEcnFunctions


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
                                      "part_name_number_path",
                                      "input_location_path",
                                      "output_path",
                                      "archive_destination"
                                  ])

        part_name_number_path = args['part_name_number_path']
        input_location_path = args['input_location_path']
        output_path = args['output_path']
        archive_destination = args['archive_destination']

        log.info("--- File Reading Started----")
        obj_list = wr.s3.list_objects(input_location_path)
        obj_list = [obj for obj in obj_list if obj.split(".")[-1] in ('xlsx', 'csv')]
        log.info(obj_list)

        ecn_combined = DCPEcnFunctions.get_combined_ecn_data(input_location_path)
        log.info("--- File Reading/Merging Complete----")

        tpn_dict = DCPEcnFunctions.get_part_num_name_dict(part_name_number_path)
        ecn_combined['temp_result'] = ecn_combined['contents_of_change'].progress_apply(
            lambda x: DCPEcnFunctions.get_part_number_name(tpn_dict, x))
        df = DCPEcnFunctions.get_final_ecn_data(ecn_combined)
        df['part_num_1'] = df['part_num_1'].progress_apply(lambda x: DCPEcnFunctions.remove_dash(x))
        df['part_num_2'] = df['part_num_2'].progress_apply(lambda x: DCPEcnFunctions.remove_dash(x))
        df['session'] = df['reg_dt'].dt.to_period('Q-MAR').dt.qyear.apply(lambda x: str(x - 1) + "-" + str(x))
        current_time = datetime.now() + timedelta(hours=5, minutes=30)  # current time Indian format
        df['glue_last_updated'] = current_time.strftime('%Y-%m-%d %H:%M:%S')


        log.info("--- Code Pre-processing Complete----")
        wr.s3.to_csv(df, path=output_path, index=False)
        log.info(f"Processed File saved to S3 path :{output_path}")
        value = 'Processed'

        # AWS Proserve : Archival code - This code is commented for testing purpose
        # today = date.today()
        # year = today.strftime("%Y")
        # month = today.strftime("%m")
        # day = today.strftime("%d")
        # archive_destination = f'{archive_destination}/year={year}/month={year}/day={year}'

        log.info("--- Archiving started----")
        DCPArchivefiles.move_s3_data(input_location_path, archive_destination, value, suffix=True)
        log.info("--- Files Archived----")

    except Exception as e:
        log.info(f"The Error={e}")
        raise e