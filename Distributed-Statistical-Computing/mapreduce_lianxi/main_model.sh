#!/bin/bash

echo "请输入taskname："; read taskname
echo "请输入mapper"; read mapper_fun
echo "请输入reducer"; read reducer_fun
echo "请输入mapper file"; read mapper_path
echo "请输入reducer file"; read reducer_path
echo "请输入input路径"; read input_path
echo "请输入output路径"; read output_path

hadoop fs -rm -r -f $output_path

hadoop jar /usr/lib/hadoop-current/share/hadoop/tools/lib/hadoop-streaming-3.1.3.jar \
    -jobconf mapred.job.name=$taskname \
    -jobconf mapred.job.priority=NORMAL \
    -jobconf mapred.map.tasks=100 \
    -jobconf mapred.reduce.tasks=10 \
    -jobconf mapred.job.map.capacity=500 \
    -jobconf mapred.job.reduce.capacity=500 \
    -jobconf stream.num.map.output.key.fields=1 \
    -jobconf num.key.fields.for.partition=1 \
    -jobconf stream.memory.limit=1000 \
    -file $mapper_path  $reducer_path \
    -output $output_path \
    -input ${input_path}  \
    -mapper $mapper_fun \
    -reducer  $reducer_fun 
    

if [ $? -ne 0 ]; then
    echo 'error'
    exit 1
fi
hadoop fs -touchz $output_path/done

exit 0

