#!/bin/bash
#
#/**
# * Copyright 2007 The Apache Software Foundation
# *
# * Licensed to the Apache Software Foundation (ASF) under one
# * or more contributor license agreements.  See the NOTICE file
# * distributed with this work for additional information
# * regarding copyright ownership.  The ASF licenses this file
# * to you under the Apache License, Version 2.0 (the
# * "License"); you may not use this file except in compliance
# * with the License.  You may obtain a copy of the License at
# *
# *     http://www.apache.org/licenses/LICENSE-2.0
# *
# * Unless required by applicable law or agreed to in writing, software
# * distributed under the License is distributed on an "AS IS" BASIS,
# * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# * See the License for the specific language governing permissions and
# * limitations under the License.
# */

cur_dir=$(dirname $0)
cur_dir=$(cd ${cur_dir}; pwd)

log_dir=$cur_dir/../logs

if [ ! -d $log_dir  ]; then
	mkdir $log_dir
fi

for( a=0; a<10; a=a+1 ); do
	HADOOP_CLIENT_OPTS="-Xmx1g -Xms512m" HADOOP_CLASSPATH="$cur_dir/../*" hadoop \
		com.trend.blueprints.util.test.GetGeneratedGraphData $@ &> $log_dir/get-generated-test-data-`date +%Y%m%d%H%M%S`.log
done
