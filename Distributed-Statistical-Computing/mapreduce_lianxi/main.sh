#!/bin/bash

PWD=$(cd $(dirname $0); pwd)
cd $PWD 1> /dev/null 2>&1

TASKNAME=stocks1
# python location on hadoop
#PY27='/fli/tools/python2.7.tar.gz'
# hadoop client
#HADOOP_HOME=/usr/lib/hadoop-current/bin/hadoop
HADOOP_INPUT_DIR1=/user/devel/2020210990Renqiang/stocks.txt
HADOOP_OUTPUT_DIR=/user/devel/2020210990Renqiang/mapreduce_lianxi/groupmean-bash+python
echo $PWD
#echo $HADOOP_HOME
echo $HADOOP_INPUT_DIR1
echo $HADOOP_OUTPUT_DIR

hadoop fs -rm  -f -r  $HADOOP_OUTPUT_DIR

hadoop jar /usr/lib/hadoop-current/share/hadoop/tools/lib/hadoop-streaming-3.1.3.jar \
    -D mapred.job.name=$TASKNAME \
    -output ${HADOOP_OUTPUT_DIR} \
    -input ${HADOOP_INPUT_DIR1}  \
    -mapper "mapper.sh" \
    -reducer "reducer.py"  \
    -file $PWD/mapper.sh  $PWD/reducer.py

if [ $? -ne 0 ]; then
    echo 'error'
    exit 1
fi
hadoop fs -touchz ${HADOOP_OUTPUT_DIR}/done

exit 0

