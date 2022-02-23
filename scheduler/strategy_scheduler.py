#!/usr/bin/env python
# -*- coding: utf-8 -*-
import re
import time
import json
import datetime
import pymysql
import traceback
import subprocess
from dateutil.parser import parse
from dateutil.relativedelta import relativedelta
from utils.config import config
from strategy_order.utils.logger_cfg import scheduler_log as _logger
from apscheduler.schedulers.background import BackgroundScheduler
from monitor.monitor_listen_port import ListenPort
from utils.redis_util import master_redis_client, report_redis_client
from strategy_order.base.order_happen_record import HappenRecord
from utils.function_util import get_scheduler_params


class StrategyScheduler():
    '''调度器 定时任务'''

    def __init__(self, scheduler_dict=None):
        self.port_status_dict = {}
        self.scheduler_day_of_month_dict = {}  # 针对每月的定时任务
        self.cleartimestamp = None
        self.scheduler_jobs = []
        self.scheduler_dict = scheduler_dict if scheduler_dict else {}
        self.master_redis_client = master_redis_client
        self.report_redis_client = report_redis_client
        self.config_scheduler_jobs = get_scheduler_params().get('scheduler_jobs', {})
        self.save_order_detail = HappenRecord()
        self.listen_port = ListenPort()
        self.scheduler = BackgroundScheduler()
        self.init_port_status_dict()
        self.init_scheduler_jobs()

    def init_port_status_dict(self):
        ''' 初始化端口服务状态 '''
        for port in self.listen_port.ping_port_array:
            self.port_status_dict[port] = None
        _logger.info('----- 初始化port_status_dict: {} -----'.format(self.port_status_dict))

    def init_cleartimestamp(self):
        ''' 初始化清理时间'''
        clear_date = None
        try:
            for i in range(3, 8):
                clear_date = datetime.date.today() + datetime.timedelta(days=-i)
                if datetime.datetime.strptime(str(clear_date), "%Y-%m-%d").weekday() == 4:  # 初始化时间，回到上周五16:00:00
                    break

            self.cleartimestamp = int(time.mktime(
                time.strptime("{} 16:00:00".format(clear_date),
                              "%Y-%m-%d %H:%M:%S")))  # 上周五 16:00:00
            _logger.info('----- 初始化清理时间: {} -----'.format(self.timestamp_to_date(self.cleartimestamp)))
            print('----- 清理日期判断: {} -----'.format(self.timestamp_to_date(self.cleartimestamp)))
        except Exception as e:
            _logger.error('----- init_cleartimestamp, {} -----'.format(traceback.format_exc()))

    def init_scheduler_jobs(self):
        ''' 初始化self.scheduler_jobs '''
        _logger.info('----- 初始化self.scheduler_jobs, scheduler_type: {} -----'.format(
            self.scheduler_dict.get('scheduler_type', None)))

        scheduler_funcitons = {'automatic_listen_port_job': self.automatic_listen_port_job,
                               'automatic_clear_redis_entrust_data': self.automatic_clear_redis_entrust_data,
                               'automatic_clear_mysql_console_procedure_data': self.automatic_clear_mysql_console_procedure_data,
                               'automatic_clear_mysql_t5_data': self.automatic_clear_mysql_t5_data,
                               'automatic_clear_mysql_t1_data': self.automatic_clear_mysql_t1_data,
                               'automatic_clear_mysql_t7_data': self.automatic_clear_mysql_t7_data,
                               'automatic_clear_redis_quotation_data': self.automatic_clear_redis_quotation_data,
                               'automatic_check_entrust_data': self.automatic_check_entrust_data,
                               'automatic_clear_files_to_be_deleted': self.automatic_clear_files_to_be_deleted,
                               'automatic_restart_docker_trade_server': self.automatic_restart_docker_trade_server}

        for scheduler_job in self.config_scheduler_jobs:
            job_id = scheduler_job.get('job_id', '').strip()
            if job_id in self.scheduler_dict.get('scheduler_jobs', []) and job_id in scheduler_funcitons.keys():  # 判断
                scheduler_job_dict = {}

                # 分interval和cron定时任务
                if scheduler_job['job_type'] == 'interval':
                    scheduler_job_dict['job_id'], scheduler_job_dict['job_function'] = job_id, scheduler_funcitons[
                        job_id]
                    scheduler_job_dict['job_type'], scheduler_job_dict['job_interval_time'] = scheduler_job.get(
                        'job_type', ''), scheduler_job.get('job_interval_time', '')
                elif scheduler_job['job_type'] == 'cron':
                    job_id = scheduler_job.get('job_id', '')
                    scheduler_job_dict['job_id'], scheduler_job_dict['job_function'] = job_id, scheduler_funcitons[
                        job_id]
                    scheduler_job_dict['job_type'], scheduler_job_dict['day_of_week'] = scheduler_job.get('job_type',
                                                                                                          ''), scheduler_job.get(
                        'job_day_of_week', '')
                    scheduler_job_dict['hour'], scheduler_job_dict['minute'] = scheduler_job.get('job_hour',
                                                                                                 ''), scheduler_job.get(
                        'job_minute', '')
                self.scheduler_jobs.append(scheduler_job_dict)

                # 针对每月的定时任务
                if scheduler_job.get('job_day_of_month', ''):
                    self.scheduler_day_of_month_dict[job_id] = scheduler_job.get('job_day_of_month', '')

        _logger.info('----- 初始化self.scheduler_day_of_month_dict完毕, {} -----'.format(self.scheduler_day_of_month_dict))
        _logger.info('----- 初始化self.scheduler_jobs完毕, {} -----'.format(self.scheduler_jobs))

    @staticmethod
    def timestamp_to_date(timestamp):
        ''' 时间戳转日期 '''
        try:
            datetime = time.strftime("%Y-%m-%d %H:%M:%S", time.localtime(int(timestamp)))
            return datetime
        except Exception as e:
            _logger.error(
                '----- 时间戳: {}, --timestamp_to_date, {} -----'.format(timestamp, traceback.format_exc()))

    def date_to_timestamp(self, index):
        ''' 日期转时间戳 '''
        try:
            before_date = datetime.date.today() + datetime.timedelta(days=index)
            timestamp = int(
                time.mktime(time.strptime("{} 00:00:00".format(before_date), "%Y-%m-%d %H:%M:%S")))  # 回到index天前的时间戳
            return timestamp  # 10位
        except Exception as e:
            _logger.error('----- date_to_timestamp, {} -----'.format(traceback.format_exc()))

    def __conn(self):
        conn = pymysql.connect(host=config.mysql_strategy_host,
                               port=config.mysql_strategy_port,
                               user=config.mysql_strategy_user,
                               password=config.mysql_strategy_password,
                               db=config.mysql_strategy_db,
                               charset=config.mysql_strategy_charset)
        cursor = conn.cursor()
        return {"conn": conn, "cursor": cursor}

    def automatic_listen_port_job(self):
        ''' 自动监听端口 '''
        _logger.info('----- 正在执行automatic_listen_port_job定时任务, {} -----'.format(self.listen_port.ping_port_array))
        for port in self.listen_port.ping_port_array:
            # _logger.info('----- 自动监听端口{} -----'.format(port))
            # print('----- 自动监听端口{} -----'.format(port))
            if self.listen_port.ping_port(int(port)):
                self.port_status_dict[int(port)] = True
            else:
                self.port_status_dict[int(port)] = False

    def automatic_clear_redis_entrust_data(self):
        ''' 自动清除redis上周的entrust数据 '''
        try:
            _logger.info('----- 正在执行automatic_clear_redis_entrust_data定时任务 -----')
            print('----- 正在执行automatic_clear_redis_entrust_data定时任务 -----')

            self.init_cleartimestamp()  # 更新清理时间
            all_keys = master_redis_client.hkeys('entrust')
            for key in all_keys:
                st = str(json.loads(master_redis_client.hget('entrust', key)).get('st', '')).split('.')[0].strip()

                curtimestamp = int(time.mktime(time.strptime(st, "%Y-%m-%d %H:%M:%S"))) if st else 0
                if curtimestamp < self.cleartimestamp:
                    self.master_redis_client.hdel('entrust', key)
                    print('----- redis 清理日期: {}, 清理键: {} -----'.format(self.timestamp_to_date(curtimestamp),
                                                                       key))  # 测试, 时间戳转日期
                    _logger.info('----- 清除redis表entrust的委托数据, {} -----'.format(key))
        except Exception as e:
            _logger.error('----- automatic_clear_redis_data, {} -----'.format(traceback.format_exc()))
            print('----- automatic_clear_redis_data, {} -----'.format(traceback.format_exc()))

    def automatic_clear_mysql_console_procedure_data(self):
        ''' 自动清除mysql上周的console_procedure数据 '''
        try:
            _logger.info('----- 正在执行automatic_clear_mysql_console_procedure_data定时任务 -----')
            print('----- 正在执行automatic_clear_mysql_console_procedure_data定时任务 -----')

            self.init_cleartimestamp()  # 更新清理时间
            connection = self.__conn()
            conn, cursor = connection['conn'], connection['cursor']

            cur_cleartimestamp = self.fill_zero(self.cleartimestamp)
            clear_sql = 'delete from CONSOLE_PROCEDURE where timestamp <= {}'.format(int(cur_cleartimestamp))
            print('----- 清除mysql表CONSOLE_PROCEDURE数据,{} -----'.format(clear_sql))  # 测试
            _logger.info('----- 清除mysql表CONSOLE_PROCEDURE数据,{} -----'.format(clear_sql))
            cursor.execute(clear_sql)
            conn.commit()
        except Exception as e:
            _logger.error('----- automatic_clear_mysql_console_procedure_data, {} -----'.format(traceback.format_exc()))
            print('----- automatic_clear_mysql_console_procedure_data, {} -----'.format(traceback.format_exc()))
        finally:
            cursor.close()
            conn.close()

    def select_mysqldb_type_tables_sql(self, type: str):
        ''' 根据type选择对应的表, sql匹配 '''
        all_tables = None
        try:
            _logger.info('----- 正在选择mysql数据库, type: {}的表 -----'.format(type))
            connection = self.__conn()
            conn, cursor = connection['conn'], connection['cursor']

            if not connection:
                return

            sql = 'select A.TABLE_NAME from INFORMATION_SCHEMA.TABLES A where A.TABLE_SCHEMA = "{}" and A.TABLE_NAME like "{}_%"'.format(
                config.mysql_strategy_db, type)
            cursor.execute(sql)
            all_tables = cursor.fetchall()
        except Exception as e:
            _logger.error('----- select_mysqldb_type_tables_sql, {} -----'.format(traceback.format_exc()))
            print('----- select_mysqldb_type_tables_sql, {} -----'.format(traceback.format_exc()))
        finally:
            cursor.close()
            conn.close()
            return all_tables

    def clear_date_judge(self, date: str, clear_interval_month: int) -> bool:
        ''' 清除日期判断 '''
        try:
            type_judge = date.split('-')
            datetime_now = datetime.datetime.now()
            datetime_interval_month_ago = datetime_now - relativedelta(months=int(clear_interval_month))
            datetime_interval_month_ago_int = int(
                ''.join(str(datetime_interval_month_ago).split(' ')[0].split('-')[: 2])) if len(
                type_judge) == 2 else int(''.join(str(datetime_interval_month_ago).split(' ')[0].split('-')))
            record_date_int = int(''.join(type_judge))
            # _logger.info(
            #     '----- 清除日期判断, 6个月前时间: {}, 数据库记录时间: {} -----'.format(datetime_interval_month_ago_int, record_date_int))
            if record_date_int < datetime_interval_month_ago_int:
                return True
            return False
        except Exception as e:
            _logger.error('----- clear_date_judge, {} -----'.format(traceback.format_exc()))
            print('----- clear_date_judge, {} -----'.format(traceback.format_exc()))

    def automatic_clear_mysql_t5_data(self):
        ''' 定时每周清除t5表的数据'''
        try:
            _logger.info('----- 正在执行automatic_clear_mysql_t5_data定时任务 -----')
            print('----- 正在执行automatic_clear_mysql_t5_data定时任务 -----')

            connection = self.__conn()
            conn, cursor = connection['conn'], connection['cursor']

            select_clear_tables = self.select_mysqldb_type_tables_sql('t5')
            _logger.info('----- 查询到mysql数据库所有t5表: {} -----'.format(select_clear_tables))

            for clear_t5_table_name in select_clear_tables:
                clear_ids, clear_t5_table_name = [], clear_t5_table_name[0]

                search_sql = 'select id, record_day from {}'.format(clear_t5_table_name)
                cursor.execute(search_sql)
                datas = cursor.fetchall()
                for data in datas:
                    id, record_day = data[0], data[1]

                    # 判断, 6个月之前的数据清除
                    if self.clear_date_judge(str(record_day).strip(), 6):
                        clear_ids.append(id)

                if clear_ids:
                    clear_sql = 'delete from {} where id = {};'.format(clear_t5_table_name, clear_ids[0]) if len(
                        clear_ids) == 1 else 'delete from {} where id in {};'.format(clear_t5_table_name,
                                                                                     tuple(clear_ids))
                    print('----- 清除mysql数据库t5表[{}]的数据, sql:{} -----'.format(clear_t5_table_name, clear_sql))
                    _logger.info('----- 清除mysql数据库t5表[{}]的数据, sql:{} -----'.format(clear_t5_table_name, clear_sql))
                    cursor.execute(clear_sql)
                else:
                    print('----- 无需清除mysql数据库t5表[{}]的数据 -----'.format(clear_t5_table_name))
                    _logger.info('----- 无需清除mysql数据库t5表[{}]的数据 -----'.format(clear_t5_table_name))
        except Exception as e:
            _logger.error('----- automatic_clear_mysql_t5_data, {} -----'.format(traceback.format_exc()))
            print('----- automatic_clear_mysql_t5_data, {} -----'.format(traceback.format_exc()))
        else:
            conn.commit()
        finally:
            cursor.close()
            conn.close()

    def automatic_clear_mysql_t1_data(self):
        ''' 定时每月清除t1表'''
        # 判断是否是当前月1号
        clear_day_of_month = self.scheduler_day_of_month_dict.get('automatic_clear_mysql_t1_data', 0)
        localtime = time.localtime(time.time())
        if int(localtime.tm_mday) != int(clear_day_of_month):
            return

        try:
            _logger.info('----- 正在执行automatic_clear_mysql_t1_data定时任务 -----')
            print('----- 正在执行automatic_clear_mysql_t1_data定时任务 -----')

            clear_tables, connection = [], self.__conn()
            conn, cursor = connection['conn'], connection['cursor']

            select_clear_tables = self.select_mysqldb_type_tables_sql('t1')
            _logger.info('----- 查询到mysql数据库所有t1表: {} -----'.format(select_clear_tables))

            for clear_table in select_clear_tables:
                clear_table = clear_table[0]
                table_date = str(clear_table).split('_')[-1]

                # 判断, 上个月之前(包含)的t1表清除
                if self.clear_date_judge(str(table_date).strip(), 0):
                    clear_tables.append('`' + clear_table + '`')

            if clear_tables:
                clear_sql = 'drop table if exists {};'.format(
                    ', '.join(clear_tables))
                print('----- 清除mysql数据库t1表,{} -----'.format(clear_sql))
                _logger.info('----- 清除mysql数据库t1表,{} -----'.format(clear_sql))
                cursor.execute(clear_sql)
            else:
                print('----- 无需清除mysql数据库t1表 -----')
                _logger.info('----- 无需清除mysql数据库t1表 -----')
        except Exception as e:
            _logger.error('----- automatic_clear_mysql_t1_data, {} -----'.format(traceback.format_exc()))
            print('----- automatic_clear_mysql_t1_data, {} -----'.format(traceback.format_exc()))
        else:
            conn.commit()
        finally:
            cursor.close()
            conn.close()

    def automatic_clear_mysql_t7_data(self):
        ''' 定时每月清除t7表'''
        # 判断是否是当前月1号
        clear_day_of_month = self.scheduler_day_of_month_dict.get('automatic_clear_mysql_t7_data', 0)
        localtime = time.localtime(time.time())
        if int(localtime.tm_mday) != int(clear_day_of_month):
            return

        try:
            _logger.info('----- 正在执行automatic_clear_mysql_t7_data定时任务 -----')
            print('----- 正在执行automatic_clear_mysql_t7_data定时任务 -----')

            clear_tables, connection = [], self.__conn()
            conn, cursor = connection['conn'], connection['cursor']

            select_clear_tables = self.select_mysqldb_type_tables_sql('t7')
            _logger.info('----- 查询到mysql数据库所有t7表: {} -----'.format(select_clear_tables))

            for clear_table in select_clear_tables:
                clear_table = clear_table[0]
                table_date = str(clear_table).split('_')[-1]

                # 判断, 上个月之前(包含)的t7表清除
                if self.clear_date_judge(str(table_date).strip(), 0):
                    clear_tables.append('`' + clear_table + '`')

            if clear_tables:
                clear_sql = 'drop table if exists {};'.format(
                    ', '.join(clear_tables))  # drop table if exists `test1`, `test2`;
                print('----- 清除mysql数据库t7表,{} -----'.format(clear_sql))
                _logger.info('----- 清除mysql数据库t7表,{} -----'.format(clear_sql))
                cursor.execute(clear_sql)
            else:
                print('----- 无需清除mysql数据库t7表 -----')
                _logger.info('----- 无需清除mysql数据库t7表 -----')
        except Exception as e:
            _logger.error('----- automatic_clear_mysql_t7_data, {} -----'.format(traceback.format_exc()))
            print('----- automatic_clear_mysql_t7_data, {} -----'.format(traceback.format_exc()))
        else:
            conn.commit()
        finally:
            cursor.close()
            conn.close()

    def automatic_clear_redis_quotation_data(self):
        ''' 定时清除当天前的行情数据 '''
        try:
            all_keys = self.report_redis_client.keys()
            timestamp = self.date_to_timestamp(-1) * 10 ** 6

            for key in all_keys:
                key = key.decode('utf-8')
                if len(key.split('.')) == 2 and key.split('.')[0].isdigit() and key.split('.')[
                    1] == 'list':  # 1001000001.list
                    # _logger.info('----- 匹配成功行情key: {}, 正在清除当天之前的数据 -----'.format(key))
                    length = self.report_redis_client.llen(key)
                    self.report_redis_client.ltrim(key, length, length)
                    _logger.info('----- 匹配成功行情key: {}, 清除键全部数据成功 -----'.format(key))
        except Exception as e:
            _logger.error('----- automatic_clear_redis_quotation_data, {} -----'.format(traceback.format_exc()))
            print('----- automatic_clear_redis_quotation_data, {} -----'.format(traceback.format_exc()))

    @staticmethod
    def date_differ(cur_date, before_date, type):
        ''' 日期相差时间 '''
        differ_time = 0  # 默认0
        try:
            cur, before = parse(cur_date), parse(before_date)
            if type == 'second':
                differ_time = (cur - before).total_seconds()  # 返回相差秒数
            elif type == 'day':
                differ_time = (cur - before).days  # 返回相差天数
        except Exception as e:
            _logger.error('----- date_differ, {} -----'.format(traceback.format_exc()))
            print('----- date_differ, {} -----'.format(traceback.format_exc()))
        finally:
            _logger.info(
                f'----- date_differ, cur_date: {cur_date}, before_date: {before_date}, differ_time: {differ_time} -----')
            return differ_time

    def automatic_check_entrust_data(self):
        ''' 定时检查委托数据, create类型且超过三分钟存异常订单记录 '''
        try:
            _logger.info('----- 正在执行automatic_check_entrust_data定时任务 -----')
            print('----- 正在执行automatic_check_entrust_data定时任务 -----')

            all_keys = master_redis_client.hkeys('entrust')
            for hash_key in all_keys:
                hash_key = hash_key.decode('utf-8')
                value_dict = json.loads(master_redis_client.hget('entrust', hash_key))
                entrust_type, st = value_dict.get('entrust_type', ''), value_dict.get('st', '')
                cur_date = time.strftime("%Y-%m-%d %H:%M:%S", time.localtime())

                # 判断, 相差秒数
                differ_second = self.date_differ(cur_date, st.split('.')[0], 'second')

                # 保存异常订单记录
                if entrust_type == 'create':
                    if differ_second >= 60 * 3:
                        self.save_order_detail.save({'type': 'abnormal_order', 'data': value_dict})
                        self.master_redis_client.hdel('entrust', hash_key)  # 异常的单删除委托数据
                        _logger.info(
                            '----- 检测委托数据订单异常, hash_key: {}, entrust_type: {}, cur_date: {}, st: {}, 时间间隔(秒): {}, 已清除 -----'.format(
                                hash_key, entrust_type, cur_date, st.split('.')[0], differ_second))
                        print(
                            '----- 检测委托数据订单异常, hash_key: {}, entrust_type: {}, cur_date: {}, st: {}, 时间间隔(秒): {}, 已清除 -----'.format(
                                hash_key, entrust_type, cur_date, st.split('.')[0], differ_second))
                    else:
                        _logger.info(
                            '----- 检测委托数据订单正常, hash_key: {}, entrust_type: {}, cur_date: {}, st: {}, 时间间隔(秒): {} -----'.format(
                                hash_key, entrust_type, cur_date, st.split('.')[0], differ_second))
        except Exception as e:
            _logger.error('----- automatic_check_entrust_data, {} -----'.format(traceback.format_exc()))
            print('----- automatic_check_entrust_data, {} -----'.format(traceback.format_exc()))

    @staticmethod
    def fill_zero(timestamp):
        if timestamp:
            timestamp = str(timestamp)
            while len(str(timestamp)) < 16:
                timestamp += '0'
        return timestamp

    def automatic_clear_files_to_be_deleted(self):
        ''' 自动清除需要删除的文件 '''
        try:
            _logger.info('----- 正在执行automatic_clear_files_to_be_deleted定时任务 -----')
            print('----- 正在执行automatic_clear_files_to_be_deleted定时任务 -----')

            clear_command_list = ['cd /data/order_execution/logs;  rm -rf info.2*.log',
                                  'cd /data/order_execution/logs; rm -rf warning.2*.log',
                                  'lsof | grep deleted | grep /data/strategy/mysql-5.7.32|cut -c 9-15|xargs kill -9',
                                  'lsof | grep deleted | grep output | cut -c 9-15|xargs kill -9']

            for clear_command in clear_command_list:
                if re.search('lsof', clear_command):
                    clear_command_list = clear_command.split('|')[: -2]
                    judge_clear_command = '|'.join(clear_command_list)  # 判断是否存在要删除的文件
                    result = subprocess.getstatusoutput(judge_clear_command)
                    if not result[1]:  # 无要删除的文件
                        _logger.info(
                            '----- automatic_clear_files_to_be_deleted, 无要删除的文件, judge_clear_command: {}, result: {} -----'.format(
                                judge_clear_command, result))
                        continue

                result = subprocess.getstatusoutput(clear_command)
                if result[0]:
                    _logger.info(
                        '----- automatic_clear_files_to_be_deleted清除失败, clear_command: {}, result: {} -----'.format(
                            clear_command, result))
                else:
                    _logger.info(
                        '----- automatic_clear_files_to_be_deleted清除成功, clear_command: {}, result: {} -----'.format(
                            clear_command, result))
        except Exception as e:
            _logger.error('----- automatic_clear_files_to_be_deleted, {} -----'.format(traceback.format_exc()))
            print('----- automatic_clear_files_to_be_deleted, {} -----'.format(traceback.format_exc()))

    def automatic_restart_docker_trade_server(self):
        ''' 自动重启docker交易服务 '''
        try:
            # 基于本地服务无需重启交易服务
            if not config.whether_docker_service: return

            _logger.info('----- 正在执行automatic_clear_files_to_be_deleted定时任务 -----')
            print('----- 正在执行automatic_clear_files_to_be_deleted定时任务 -----')

            restart_command_list = ['docker restart order_execution']

            for restart_command in restart_command_list:
                result = subprocess.getstatusoutput(restart_command)
                if result[0]:
                    _logger.info(
                        '----- automatic_restart_docker_trade_server重启失败, restart_command: {}, result: {} -----'.format(
                            restart_command, result))
                else:
                    _logger.info(
                        '----- automatic_restart_docker_trade_server重启成功, restart_command: {}, result: {} -----'.format(
                            restart_command, result))
        except Exception as e:
            _logger.error('----- automatic_restart_docker_trade_server, {} -----'.format(traceback.format_exc()))
            print('----- automatic_restart_docker_trade_server, {} -----'.format(traceback.format_exc()))

    def start_scheduler_job(self):
        ''' 启动定时任务 '''
        try:
            print('***** 开始启动定时任务, scheduler_dict: {} *****'.format(self.scheduler_dict))
            _logger.info('***** 开始启动定时任务, scheduler_dict: {} *****'.format(self.scheduler_dict))

            for scheduler_job in self.scheduler_jobs:
                if not self.is_exist_scheduler_job(scheduler_job['job_id']):
                    if scheduler_job['job_type'] == 'interval':
                        print('----- 开始启动interval定时任务{} -----'.format(scheduler_job['job_id']))
                        _logger.info('----- 开始启动interval定时任务{} -----'.format(scheduler_job['job_id']))
                        # 在监听之前先调用一次
                        scheduler_job['job_function']()
                        self.scheduler.add_job(scheduler_job['job_function'], scheduler_job['job_type'],
                                               id=scheduler_job['job_id'], max_instances=1,
                                               seconds=int(scheduler_job['job_interval_time']))
                    elif scheduler_job['job_type'] == 'cron':
                        print('----- 开始启动cron定时任务{} -----'.format(scheduler_job['job_id']))
                        _logger.info('----- 开始启动cron定时任务{} -----'.format(scheduler_job['job_id']))
                        self.scheduler.add_job(scheduler_job['job_function'], scheduler_job['job_type'],
                                               day_of_week=scheduler_job['day_of_week'], hour=scheduler_job['hour'],
                                               minute=scheduler_job['minute'])
            self.scheduler.start()
        except Exception as e:
            _logger.error('----- start_scheduler_job {} -----'.format(traceback.format_exc()))

    def is_exist_scheduler_job(self, scheduler_job_id):
        ''' 是否存在定时任务 '''
        if self.scheduler.get_job(scheduler_job_id):
            return True
        return False


if __name__ == '__main__':
    obj = StrategyScheduler()
    obj.start_scheduler_job()
