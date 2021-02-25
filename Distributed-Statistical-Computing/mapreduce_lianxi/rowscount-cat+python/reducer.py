#! /usr/bin/env python3

import sys

count = 0 
data = []
col_num_min = 0
col_num_max = 0
for line in sys.stdin:
	col_num = len(line.split(','))
	count += 1
	data.append(line)
	if count ==1:
		col_num_min = col_num
		col_num_max = col_num
	if col_num < col_num_min:
		col_num_min = col_num
	if col_num > col_num_max:
		col_num_max = col_num
print('%d\t%d\t%d'%(count,col_num_min,col_num_max))



