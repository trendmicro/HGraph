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

log_dir=$cur_dir/../../logs

if [ ! -d $log_dir  ]; then
        mkdir $log_dir
fi

export HADOOP_CLASSPATH="$cur_dir/../../*:`hbase classpath`"
#export HADOOP_ROOT_LOGGER="TRACE,console"

hadoop org.trend.hgraph.mapreduce.pagerank.GetNoColumnsRows $* &> $log_dir/get-no-column-rows-`date +%Y%m%d%H%M%S`.log
