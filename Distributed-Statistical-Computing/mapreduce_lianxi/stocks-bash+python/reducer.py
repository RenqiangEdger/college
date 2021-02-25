#! /usr/bin/env python3

import sys

stocks_num = {}
stocks_value = {}
for line in sys.stdin:
	names, date, value1,value2 = line.split('\t')
	value1 = float(value1)
	value2 = float(value2)
	stocks_num[names] = stocks_num.get(names,0) + 1 
	stocks_value[names] = stocks_value.get(names,0) + value1

for key, value in stocks_value.items():
	print('%s\t%f\n'%(key,value/(stocks_num.get(key))))
