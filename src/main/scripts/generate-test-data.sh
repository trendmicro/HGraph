#!/bin/bash

cur_dir=$(dirname $0)
cur_dir=$(cd ${cur_dir}; pwd)

log_dir=$cur_dir/../logs

if [ ! -d $log_dir  ]; then
	mkdir $log_dir
fi


HADOOP_CLIENT_OPTS="-Xmx1g -Xms512m" HADOOP_CLASSPATH="$cur_dir/../*" hadoop \
	com.trend.blueprints.util.test.GenerateTestData $@ &> $log_dir/generate-test-data-`date +%Y%m%d%H%M%S`.log
