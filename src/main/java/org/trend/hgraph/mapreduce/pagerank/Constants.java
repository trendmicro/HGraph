/**
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.trend.hgraph.mapreduce.pagerank;

/**
 * A Constants-central class for pageRank MR jobs.
 * @author scott_miao
 */
public class Constants {
  public static final String PAGE_RANK_HDFS_PATH_KEY = "mr.pagerank.hdfs.path";

  public static final String PAGE_RANK_DAMPING_FACTOR_KEY = "mr.pagerank.damping.factor";

  public static final double PAGE_RANK_DAMPING_FACTOR_DEFAULT_VALUE = 0.85D;

  public static final String PAGE_RANK_VERTICES_TOTAL_COUNT_KEY = "mr.pagerank.vertices.total.count";
}
