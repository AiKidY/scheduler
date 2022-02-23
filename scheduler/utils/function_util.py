#!/usr/bin/env python
# -*- coding: utf-8 -*-
import re
import json
import pymysql
import uuid
import time

from utils.config import config as __config
from utils.datetime_util import okex_contract_type
from dateutil.parser import parse
from utils.redis_util import master_redis_client
from utils.const import const

def get_scheduler_params():
    scheduler_params = {
        'scheduler_jobs': [{'job_id': 'automatic_listen_port_job', 'job_type': 'interval', 'job_interval_time': 10},
                           {'job_id': 'automatic_clear_redis_entrust_data', 'job_type': 'cron',
                            'job_day_of_week': 'fri',
                            'job_hour': '15', 'job_minute': '55'},
                           {'job_id': 'automatic_clear_mysql_console_procedure_data', 'job_type': 'cron',
                            'job_day_of_week': 'fri', 'job_hour': '15', 'job_minute': '55'},
                           {'job_id': 'automatic_clear_mysql_t5_data', 'job_type': 'cron', 'job_day_of_week': 'fri',
                            'job_hour': '15', 'job_minute': '55'},
                           {'job_id': 'automatic_clear_mysql_t1_data', 'job_type': 'cron', 'job_day_of_month': '1',
                            'job_day_of_week': '*', 'job_hour': '06', 'job_minute': '00'},
                           {'job_id': 'automatic_clear_mysql_t7_data', 'job_type': 'cron', 'job_day_of_month': '1',
                            'job_day_of_week': '*', 'job_hour': '06', 'job_minute': '00'},
                           {'job_id': 'automatic_clear_redis_quotation_data', 'job_type': 'cron',
                            'job_day_of_week': '*',
                            'job_hour': '06', 'job_minute': '00'},
                           {'job_id': 'automatic_check_entrust_data', 'job_type': 'interval', 'job_interval_time': 60},
                           {'job_id': 'automatic_clear_files_to_be_deleted', 'job_type': 'cron',
                            'job_day_of_week': '*', 'job_hour': '06', 'job_minute': '00'},
                           {'job_id': 'automatic_restart_docker_trade_server', 'job_type': 'cron',
                            'job_day_of_week': 'fri', 'job_hour': '15', 'job_minute': '55'}]}
    return scheduler_params
