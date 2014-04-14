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

export conf_dir=$cur_dir/../../conf
source $conf_dir/hgraph-env.sh

HADOOP_CLIENT_OPTS="$HADOOP_CLIENT_OPTS -Dhgraph.log.file=reset-pr-update-flag.log" hadoop org.trend.hgraph.mapreduce.pagerank.ResetPageRankUpdateFlag $*
