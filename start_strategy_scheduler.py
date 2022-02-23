#!/usr/bin/env python
# -*- coding: utf-8 -*-
import time
from strategy_order.base.strategy_scheduler import StrategyScheduler


class Main:
    def __init__(self):
        self.schedule_dict = {'scheduler_type': 'scheduler_jobs',
                              'scheduler_jobs': ['automatic_clear_redis_entrust_data',
                                                 'automatic_clear_mysql_console_procedure_data',
                                                 'automatic_clear_mysql_t5_data',
                                                 'automatic_clear_mysql_t1_data',
                                                 'automatic_clear_mysql_t7_data',
                                                 'automatic_clear_redis_quotation_data',
                                                 'automatic_check_entrust_data',
                                                 'automatic_clear_files_to_be_deleted',
                                                 'automatic_restart_docker_trade_server']}
        self.strategy_scheduler = StrategyScheduler(self.schedule_dict)

    def start_strategy_scheduler(self):
        self.strategy_scheduler.start_scheduler_job()
        while True:
            time.sleep(10)


'''------------***定时任务scheduler_jobs***-----------------'''
if __name__ == '__main__':
    obj = Main()
    obj.start_strategy_scheduler()