package com.trend.blueprints.util.test;

import static org.junit.Assert.*;

import java.util.List;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.util.Bytes;
import org.junit.After;
import org.junit.AfterClass;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;

import com.trend.blueprints.HBaseGraphConstants;
import com.trend.blueprints.test.AbstractHBaseMiniClusterTest;

public class GetGeneratedGraphDataTest extends AbstractHBaseMiniClusterTest {
  
  
  @BeforeClass
  public static void setUpBeforeClass() throws Exception {
    AbstractHBaseMiniClusterTest.setUpBeforeClass();
    Configuration conf = TEST_UTIL.getConfiguration();
    createTable(conf, Bytes.toBytes("test.vertex"), 
        transfer2BytesArray(new String[] {"property"}));
    createTable(conf, Bytes.toBytes("test.edge"), 
        transfer2BytesArray(new String[] {"property"}));
    
    conf.set(HBaseGraphConstants.HBASE_GRAPH_TABLE_VERTEX_NAME_KEY, "test.vertex");
    conf.set(HBaseGraphConstants.HBASE_GRAPH_TABLE_EDGE_NAME_KEY, "test.edge");
    
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

  @Test
  public void testRun() throws Exception {
    // initial test data
    Configuration conf = TEST_UTIL.getConfiguration();
    GenerateTestData genTestData = new GenerateTestData();
    genTestData.setConf(conf);
    genTestData.run(new String[] {"-v", "500", "test.vertex", "test.edge"});
    
    List<String> firstVertices = genTestData.getFirstVertices();
    assertEquals(1, firstVertices.size());
    
    GetGeneratedGraphData getData = new GetGeneratedGraphData();
    getData.setConf(conf);
    getData.run(new String[] {"-i", firstVertices.get(0), "test.vertex", "test.edge"});
  }

}
