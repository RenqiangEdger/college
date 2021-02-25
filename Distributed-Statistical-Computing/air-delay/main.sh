#! /bin/bash

PWD=$(cd $(dirname $0); pwd)
cd $PWD 1> /dev/null 2>&1

PYSPARK_PYTHON=python3.6 spark-submit\
   --master yarn \
   $PWD/preprocessing.py
