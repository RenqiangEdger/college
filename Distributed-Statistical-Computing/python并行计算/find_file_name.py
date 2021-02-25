# !/D:/Anaconda/ana/python.exe
# -*- coding:utf-8 -*-
# author: RQ ; time:2020/9/17

import os
import pandas as pd
import time
from multiprocessing import Pool


'''
def find_file_paths(file_name, file_type='csv', location_path='D:\\'):
    start = time.clock()
    files_paths = list(os.walk(location_path))
    regx = '.'+file_name+'.'+file_type
    result = []
    for node in files_paths:
        if len(node[-1]):
            file = pd.Series(node[-1])
            loc = file.str.contains(regx)
            if any(loc):
                 result = result + [node[0] +'/'+ s for s in file[loc]]
            else:
                continue
        else:
            continue
    end = time.clock()
    return {'path':result, 'time(s)': end-start}
    


def find_file_paths(kw):
    # kw是一个字典，关键字file_name：查询文件名；file_type：查询文件类型；path：查询文件位置
    start = time.clock()
    files_paths = list(os.walk(kw['path']))
    if len(kw) == 3:
        regx = '.' + kw['file_name'] + '.' + kw['file_type']
    elif 'file_name' in kw.keys():
        regx = '.'+kw['file_name']+'.'
    else:
        regx = '.' + kw['file_type']

    result = []
    for node in files_paths:
        if len(node[-1]):
            file = pd.Series(node[-1])
            loc = file.str.contains(regx)
            if any(loc):
                 result = result + [node[0]+'/'+s for s in file[loc]]
            else:
                continue
        else:
            continue
    end = time.clock()
    return {'path': result, 'time(s)': end-start}
'''

def find_file_paths(path, file_name=None, file_type=None):
    start = time.process_time()
    start1 = time.time()
    result = []
    for root, dirs, files in os.walk(path):
        for file in files:
            name, types = os.path.splitext(file)
            if file_type is not None:
                if file_name is not None:
                    if (file_name in file) & (file_type in types):
                        result.append(os.path.join(root, file))
                elif file_type in types:
                    result.append(os.path.join(root, file))
            elif file_name in file:
                result.append(os.path.join(root, file))
    end = time.process_time()
    end1 = time.time()
    return {'path': result, 'time_cpu(s)': end-start, 'time_pro(s)': end1-start1}

