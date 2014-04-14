package org.trend.hgraph.mapreduce.pagerank;


import org.apache.hadoop.util.Tool;
import org.junit.After;
import org.junit.AfterClass;
import org.junit.Assert;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;
import org.trend.hgraph.HBaseGraphConstants;
import org.trend.hgraph.test.AbstractHBaseMiniClusterTest;

public class ResetPageRankUpdateFlagTest extends AbstractHBaseMiniClusterTest {
  
  private static final String TABLE = "test.vertex-v01";
  private static final String[] CF = { HBaseGraphConstants.HBASE_GRAPH_TABLE_COLFAM_PROPERTY_NAME };

  @BeforeClass
  public static void setUpBeforeClass() throws Exception {
    AbstractHBaseMiniClusterTest.setUpBeforeClass();
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
    importData(new String[] { "-Dimporttsv.columns=HBASE_ROW_KEY," + CF[0] + ":pageRank@String",
        "-Dimporttsv.separator=|" }, TABLE, CF,
      "org/trend/hgraph/mapreduce/pagerank/vertex-test-01.data");
    printTable(TABLE);
    
    Tool tool = new ResetPageRankUpdateFlag(TEST_UTIL.getConfiguration());
    int status = tool.run(new String[] { TABLE });
    Assert.assertEquals(0, status);

    printTable(TABLE);
  }

}
