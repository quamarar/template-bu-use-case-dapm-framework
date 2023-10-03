#!/usr/bin/env python3

import json
import logging
import argparse
from utils import jenkins_utils


if __name__ == '__main__':
    example = '''Example:
    export PYTHONPATH="${PWD}:$PYTHONPATH" # This is to import utils
    python step_function_executioner.py --env-file ../../env/jenkins.tfvars.json
'''
    parser = argparse.ArgumentParser(description='Execute step function passing parameters from json env',
                                     epilog=example,
                                     formatter_class=argparse.RawDescriptionHelpFormatter)
    parser.add_argument('--env-file', type=str, required=True)
    args = parser.parse_args()

    logging.disable()

    name_prefix = jenkins_utils.get_name_prefix(args.env_file)

    buckets = {
        "training_internal_bucket": f"{name_prefix}-training-internal",
        "inferencing_internal_bucket": f"{name_prefix}-inferencing-internal",
        "analytics_etl_bucket": f"{name_prefix}-analytics-etl",
    }

    print(json.dumps(buckets))
