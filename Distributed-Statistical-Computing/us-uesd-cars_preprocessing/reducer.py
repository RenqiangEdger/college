#! /usr/bin/env python3
import sys
import numpy as np

xty_xtx = []
p = 31
for line in sys.stdin:
    xty_xtx.append(line.strip().split('\t'))
    
xty_xtx = np.array(xty_xtx,dtype=np.float64)
xty_xtx = np.sum(xty_xtx,axis=0)
xty_xtx = xty_xtx.reshape((p,-1),order="F")
xtx =  xty_xtx[:,1:]
xty = xty_xtx[:,0]
beta_hat = np.dot(np.linalg.inv(xtx),xty)
s_format1 = '\t'.join(['%f']*len(beta_hat))
print(s_format1%tuple(beta_hat))





