#!/bin/bash

PWD=$(cd $(dirname $0); pwd)
cd $PWD 1> /dev/null 2>&1

TASKNAME=preprocess1

#HADOOP_HOME=/usr/lib/hadoop-current/bin/hadoop

HADOOP_INPUT_DIR1=/user/devel/data/used_cars_data.csv
HADOOP_OUTPUT_DIR=/user/devel/2020210990Renqiang/us-used-cars/preprocess2
echo $PWD
#echo $HADOOP_HOME
echo $HADOOP_INPUT_DIR1
echo $HADOOP_OUTPUT_DIR

hadoop fs -rm -f -r $HADOOP_OUTPUT_DIR

hadoop jar /usr/lib/hadoop-current/share/hadoop/tools/lib/hadoop-streaming-3.1.3.jar \
    -D mapred.job.name=$TASKNAME \
    -D mapred.reduce.tasks=1 \
    -file $PWD/mapper.py  $PWD/reducer.py  \
    -output ${HADOOP_OUTPUT_DIR} \
    -input ${HADOOP_INPUT_DIR1}  \
    -mapper "python3.8  mapper.py" \
    -reducer "python3.8 reducer.py" 
    

if [ $? -ne 0 ]; then
    echo 'error'
    exit 1
fi
hadoop fs -touchz ${HADOOP_OUTPUT_DIR}/done

exit 0

