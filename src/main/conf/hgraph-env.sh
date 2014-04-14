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


log_dir=$conf_dir/../logs

if [ ! -d $log_dir  ]; then
        mkdir $log_dir
fi

export HGRAPH_LOG_DIR=$log_dir
HADOOP_CLIENT_OPTS="-Dlog4j.configuration=conf/log4j.properties -Dhgraph.log.dir=$HGRAPH_LOG_DIR"
export HADOOP_CLASSPATH="$conf_dir/../:$conf_dir/../*:`hbase classpath`"