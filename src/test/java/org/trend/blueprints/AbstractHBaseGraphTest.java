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
package org.trend.blueprints;

import org.apache.hadoop.conf.Configuration;
import org.junit.After;
import org.junit.AfterClass;
import org.junit.Before;
import org.junit.BeforeClass;
import org.trend.blueprints.test.AbstractHBaseMiniClusterTest;

public abstract class AbstractHBaseGraphTest extends AbstractHBaseMiniClusterTest {
  
  @BeforeClass
  public static void setUpBeforeClass() throws Exception {
    AbstractHBaseMiniClusterTest.setUpBeforeClass();
    importData(
        new String[] {
            "-Dimporttsv.columns=HBASE_ROW_KEY,property:name@String,property:lang@String,property:age@String", 
            "-Dimporttsv.separator=|"}, 
        "test.vertex", 
        new String[] {"property"}, 
        "org/trend/blueprints/vertex-test.data");
    
    importData(
        new String[] {
            "-Dimporttsv.columns=HBASE_ROW_KEY,property:weight@String", 
            "-Dimporttsv.separator=|"}, 
        "test.edge", 
        new String[] {"property"}, 
        "org/trend/blueprints/edge-test.data");
    
    printTable("test.vertex");
    printTable("test.edge");
    
    Configuration config = TEST_UTIL.getConfiguration();
    config.set(HBaseGraphConstants.HBASE_GRAPH_TABLE_VERTEX_NAME_KEY, "test.vertex");
    config.set(HBaseGraphConstants.HBASE_GRAPH_TABLE_EDGE_NAME_KEY, "test.edge");
  }

  @AfterClass
  public static void tearDownAfterClass() throws Exception {
    AbstractHBaseMiniClusterTest.tearDownAfterClass();
  }

  @Before
  public void setUp() throws Exception {
  }

  @After
  public void tearDown() throws Exception {
  }

}
