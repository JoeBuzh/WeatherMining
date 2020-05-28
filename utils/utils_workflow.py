# -*- encoding: utf-8 -*-
'''
@Filename    : utils_workflow.py
@Datetime    : 2020/05/11 09:13:36
@Author      : Joe-Bu
@version     : 1.0
@description : 提取工具
'''

import os
import sys
import calendar
import traceback
from copy import deepcopy
from datetime import datetime, timedelta

import numpy as np
import pandas as pd
import psycopg2


def checkdir(path: str):
    '''
        根据给定路径，判断是否存在目前，不存在则创建该目录;
        path -> abspath;
    '''
    if not os.path.exists(path):
        os.makedirs(path)

    assert os.path.exists(path)

    return path


def get_cursor(db):
    '''
        获取数据库连接connect、coursor
    '''
    connet = psycopg2.connect(
        database=db['database'], 
        user=db['user'], 
        password=db['password'], 
        host=db['host'], 
        port=db['port'])
    cursor = connet.cursor()

    return connet, cursor


def sql_qc_stats() -> str:
    '''
        获取某个月份表下站点质控类型数据分布
    '''
    sql = '''
        SELECT b."name", a.* from (
            SELECT DISTINCT qccode, count(value) 
            FROM data_{0}
            GROUP BY qccode) as a
        LEFT JOIN qccode as b on a.qccode=b.qccode;
    '''.format('201912')

    return sql


def get_site_local(filename: str):
    '''
        Open local file by pandas read_csv
    '''
    assert os.path.exists(filename)
    data = pd.read_csv(filename, sep=',', encoding='utf-8')

    return data


def get_month_range(start, t_delta: int=None):
    '''
        给定当月开始时间（当月第一天零点）
        返回下个月第一天零点
    '''
    _, days = calendar.monthrange(start.year, start.month)
    if t_delta:
        end = start + timedelta(hours=24)
    else:
        end = start + timedelta(days=days)
    
    return end


def fetch_data(db, sql):
    '''
        根据sql实现取数据，返回生成的dataframe.
        最主要sql工具
        db  数据库信息
        sql 查询语句
    '''
    try:
        connet, cursor = get_cursor(db)
        sql = sql
        cursor.execute(sql)
        data = cursor.fetchall()
        cols = cursor.description
    except Exception as e_db:
        traceback.print_exc(e_db)
    else:
        if data:
            col = [i.name for i in cols]
            df = pd.DataFrame(data, columns=col)
            return True, df
        else:
            print("No Data!")
            # cursor.rollback()
            return False, None
    finally:
        cursor.close()
        connet.close()


def sql_active_station(month: str) -> str:
    '''
        获取每月有数据记录的站点及数据量信息
    '''
    sql = '''
        SELECT DISTINCT stationcode, count(value) from data_{}
        GROUP BY stationcode;
    '''.format(month)

    return sql


def get_code_from(db_info, month, localfile=None, src='local'):
    '''
        根据src不同选择基于本地文件或者分月表实际情况的站点;
        month:      for src='db';
        localfile:  for scr='local;
        return:     stationcode list;
    '''
    assert src in ['local', 'db']
    if localfile:
        assert os.path.exists(localfile)

    if src == 'local':
        data = get_site_local(localfile)
    elif src == 'db':
        sql = sql_active_station(month)
        if fetch_data(db_info, sql)[0]:
            _, data = fetch_data(db_info, sql)
    else:
        pass

    return data