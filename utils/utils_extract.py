# -*- encoding: utf-8 -*-
'''
@Filename    : utils_extract.py
@Datetime    : 2020/05/16 14:46:03
@Author      : Joe-Bu
@version     : 1.0
'''

import os
import sys

import pandas as pd


def get_site_local(filename: str):
    '''
        Open local file by pandas read_csv
    '''
    assert os.path.exists(filename)
    data = pd.read_csv(filename, sep=',', encoding='utf-8')

    return data


def checkdir(path: str):
    '''
        根据给定路径，判断是否存在目前，不存在则创建该目录;
        path -> abspath;
    '''
    if not os.path.exists(path):
        os.makedirs(path)

    assert os.path.exists(path)

    return path


def get_code_from(localfile=None):
    '''
        localfile
        return:     stationcode list;
    '''
    if localfile:
        assert os.path.exists(localfile)    
        data = get_site_local(localfile)
        
    return data