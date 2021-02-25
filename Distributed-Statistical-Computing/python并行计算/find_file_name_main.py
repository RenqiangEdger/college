# !/D:/Anaconda/ana/python.exe
# -*- coding:utf-8 -*-
# author: RQ ; time:2020/9/21

import os
import time
from multiprocessing import Pool
from homework.find_file_name import find_file_paths
import numpy as np
import matplotlib.pyplot as plt


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


t1 = time.time()
if __name__ == '__main__':
    __spec__ = "ModuleSpec(name='builtins', loader=<class '_frozen_importlib.BuiltinImporter'>)"
    n = 15
    time_total = []
    time_sub_pro = []
    time_sub_cpu = []
    for i in range(1, n):
        location = ['D:\\'] * i
        file_name = ['数据'] * i
        file_type = ['csv'] * i
        start1 = time.time()
        with Pool(processes=i) as pool:
            result1 = pool.starmap(find_file_paths, zip(location, file_name, file_type))
        pool.close()
        pool.join()
        end1 = time.time()
        t = end1 - start1
        time_total.append(t)
        subpro_max_time = np.max(np.array([d['time_pro(s)'] for d in result1]))
        subcpu_max_time = np.max(np.array([d['time_cpu(s)'] for d in result1]))
        time_sub_cpu.append(subcpu_max_time)
        time_sub_pro.append(subpro_max_time)

t2 = time.time()
t2-t1

time_total = np.array(time_total)
time_sub_cpu = np.array(time_sub_cpu)
time_sub_pro = np.array(time_sub_pro)
time_delta = time_total - time_sub_cpu
'''
fig = plt.figure(1)
ax1 = plt.subplot(1, 2, 1)
plt.plot(np.arange(1, n), time_delta)
plt.xlabel("The numbers of processing")
plt.ylabel('Time of communication:s')
x_major_locator = plt.MultipleLocator(2)
ax=plt.gca()
#ax为两条坐标轴的实例
ax.xaxis.set_major_locator(x_major_locator)
ax2 = plt.subplot(1, 2, 2)
plt.plot(np.arange(1, n), time_total/(time_sub_pro[0]*np.arange(1,n)))
plt.xlabel("The numbers of processing")
plt.ylabel('The efficiency of muti-processing')
x_major_locator = plt.MultipleLocator(2)
ax=plt.gca()
#ax为两条坐标轴的实例
ax.xaxis.set_major_locator(x_major_locator)
plt.show()
'''
plt.plot(np.arange(1, n), time_delta)
plt.xlabel("The numbers of processing")
plt.ylabel('Time of communication:s')  # 并行计算时间-cpu运行时间
plt.show()

find_file_paths('D:\\', file_name='数据', file_type='csv')
time_single = []
for i in range(1, n):
    n = i
    location = ['D:\\']*n
    file_name = ['数据']*n
    file_type = ['csv']*n
    start = time.time()
    result = list(map(lambda x, y, z: find_file_paths(x, y, z), location, file_name, file_type))
    end = time.time()
    time_single.append(end-start)

#  plt.plot(np.arange(1, n), time_total/(time_sub_pro[0]*np.arange(1, n)))
plt.plot(np.arange(1, n), time_total/time_single)
plt.xlabel("The numbers of processing")
plt.ylabel('The efficiency of muti-processing')  # 并行计算时间/(进程数*单个运行时间)
plt.show()


dir_path = np.array(os.listdir("D:\\"))
index = [j for j in range(len(dir_path)) if os.path.isdir("D:\\"+dir_path[j])]
location = ["D:\\"+dd for dd in dir_path[index]]
file_name = ['数据'] * len(location)
file_type = ['csv'] * len(location)
time_record = []
for i in range(1, len(location)+1):
    t1 = time.time()
    if __name__ == '__main__':
        __spec__ = "ModuleSpec(name='builtins', loader=<class '_frozen_importlib.BuiltinImporter'>)"
        with Pool(processes=i) as pool:
            result1 = pool.starmap(find_file_paths, zip(location, file_name, file_type))
        pool.close()
        pool.join()
    t2 = time.time()
    time_record.append(t2-t1)


plt.plot(np.arange(1, len(location)+1), time_record)
plt.xlabel("The numbers of processing")
plt.ylabel('Time:s')  # 并行计算时间-cpu运行时间
plt.axis([1, len(location), 2, 14])
plt.axhline(y=4.59375, ls=":", c="red")
plt.show()





t1 = time.time()
if __name__ == '__main__':
    __spec__ = "ModuleSpec(name='builtins', loader=<class '_frozen_importlib.BuiltinImporter'>)"
    n = 4
    location = ['D:\\'] * n
    file_name = ['数据'] * n
    file_type = ['csv'] * n

    with Pool(processes=n) as pool:
        result1 = pool.starmap(find_file_paths, zip(location, file_name, file_type))
    pool.close()
    pool.join()

t2 = time.time()
t2-t1




pool = Pool(processes=36)
len(pool._pool)
dir(pool)
pool._processes
pool.close()
pool.join()