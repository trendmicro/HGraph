#!/bin/bash

cur_dir=$(dirname $0)
cur_dir=$(cd ${cur_dir}; pwd)

log_dir=$cur_dir/../../logs

if [ ! -d $log_dir  ]; then
        mkdir $log_dir
fi

export HADOOP_CLASSPATH="$cur_dir/../../*:`hbase classpath`"
#export HADOOP_ROOT_LOGGER="TRACE,console"

hadoop org.trend.hgraph.mapreduce.lib.input.CalculateInputSplitMapper $* &> $log_dir/calc-input-split-mapper-`date +%Y%m%d%H%M%S`.log

