# -*- encoding: utf-8 -*-
'''
@Filename    : utils.py
@Datetime    : 2020/05/11 09:13:36
@Author      : Joe-Bu
@version     : 1.0
@description : 前期分析工具，非流程调用
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


def sql_get_site() -> str:
    '''
        获取数据库目前所有组分观测站点的信息sql.
    '''
    sql = '''
        select * from site;
    '''
    return sql


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
            cursor.rollback()
            return False, None
    finally:
        cursor.close()
        connet.close()


def get_stationcode(site_local, site_db):
    '''
        给原始站点数据匹配stationcode
        非重复调用功能
    '''
    for _, (s_code, s_post) in enumerate(zip(site_local['UniqueCode'], site_local['Position'])):
        code =site_db.loc[
            (site_db['statecode']==str(s_code))&(site_db['name']==s_post), 'stationcode'].values[0]
        if code:
            site_local.loc[
                (site_local['UniqueCode']==s_code)&(site_local['Position']==s_post), 'Stationcode'] = code
        else:
            continue

    return site_local


def match_stationcode():
    '''
        获取所有站点的stationcode
        匹配站点信息code
        便于从组分数据分月表中提取相应站点的数据
        *** 非重复调用功能
    '''
    site_file = r'/Users/joe/Desktop/江苏组分数据/站点信息.csv'
    site_local = get_site_local(site_file)
    site_sql = sql_get_site()
    _, site_db = fetch_data(db, site_sql)
    site_new = get_stationcode(site_local, site_db)
    site_new.to_csv('/Users/joe/Desktop/江苏组分数据/站点信息new.csv', encoding='utf-8')


def sql_site_data(month: str, compose: dict, stationcode: str) -> str:
    '''
        获取固定站点在分月表中的
    '''
    id_tuple = tuple(k for k, _ in compose.items())
    sql = '''
        select a.
        from data_{0} as a
        left join parameter as b on a.parameterid=b.parameterid
        left join site as c ON a.stationcode=c.stationcode
        where a.parameterid in {1}
        and a.stationcode='{2}'
        and a.qccode=0;
    '''.format(month, id_tuple, stationcode)

    return sql


def sql_active_station(month: str) -> str:
    '''
        获取每月有数据记录的站点及数据量信息
    '''
    sql = '''
        SELECT DISTINCT stationcode, count(value) from data_{}
        GROUP BY stationcode;
    '''.format(month)

    return sql


def stats_month_active(month):
    '''
        统计每月活跃的站点及数据记录情况，形成csv文件.
        本地依据参考的站点信息
        site_info = get_site_local('/Users/joe/Desktop/江苏组分数据/站点信息new.csv')
        逐月loop
    '''
    active_list = []
    for _, month in enumerate(month):
        sql_active = sql_active_station(month)
        if fetch_data(sql_active)[0]:
            df_active = fetch_data(sql_active)[1]
            active_list.append(df_active)
    df_base = deepcopy(active_list[0])
    for _, df in enumerate(active_list[1:]):
        df_base = df_base.merge(df, how='outer', left_on='stationcode', right_on='stationcode')
    df_base.to_csv('./active.csv', encoding='utf-8')


def get_code_from(db_info, month, localfile=None, src='local'):
    '''
        根据src不同选择基于本地文件或者分月表实际情况的站点;
        month:      for src='db';
        localfile:  for scr='local;
        return:     stationcode list;
    '''
    assert src in ['local', 'db']
    assert os.path.exists(localfile)

    if src == 'local':
        data = get_site_local(localfile)
    elif src == 'db':
        sql = sql_active_station(month)
        if fetch_data(db_info, sql)[0]:
            _, data = fetch_data(db_info, sql)

    return data['stationcode']