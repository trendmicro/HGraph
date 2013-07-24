package com.trend.blueprints;

import org.apache.hadoop.conf.Configuration;
import org.junit.After;
import org.junit.AfterClass;
import org.junit.Before;
import org.junit.BeforeClass;

import com.trend.blueprints.test.AbstractHBaseMiniClusterTest;

public abstract class AbstractHBaseGraphTest extends AbstractHBaseMiniClusterTest {
  
  @BeforeClass
  public static void setUpBeforeClass() throws Exception {
    AbstractHBaseMiniClusterTest.setUpBeforeClass();
    importData(
        new String[] {
            "-Dimporttsv.columns=HBASE_ROW_KEY,property:name,property:lang@String,property:age@String", 
            "-Dimporttsv.separator=|"}, 
        "test.vertex", 
        new String[] {"property"}, 
        "com/trend/blueprints/vertex-test.data");
    
    importData(
        new String[] {
            "-Dimporttsv.columns=HBASE_ROW_KEY,property:weight@String", 
            "-Dimporttsv.separator=|"}, 
        "test.edge", 
        new String[] {"property"}, 
        "com/trend/blueprints/edge-test.data");
    
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
