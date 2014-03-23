package org.trend.hgraph.mapreduce.lib.input;

import java.io.IOException;
import java.io.InputStream;

import org.apache.commons.io.IOUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.junit.After;
import org.junit.AfterClass;
import org.junit.Assert;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;
import org.trend.hgraph.AbstractHBaseGraphTest;
import org.trend.hgraph.HBaseGraphConstants;
import org.trend.hgraph.test.AbstractHBaseMiniClusterTest;

public class DriverTest extends AbstractHBaseGraphTest {

  @BeforeClass
  public static void setUpBeforeClass() throws Exception {
    AbstractHBaseMiniClusterTest.setUpBeforeClass();
  }

  @AfterClass
  public static void tearDownAfterClass() throws Exception {
    AbstractHBaseGraphTest.tearDownAfterClass();
  }

  @Before
  public void setUp() throws Exception {
  }

  @After
  public void tearDown() throws Exception {
  }

  @Test
  public void testRun_b1_m3() throws Exception {
    // init test table
    String tableName = "test.vertex-01";
    String outputPath = "/test-01";
    createTestTable(tableName, 3);

    // start test
    Configuration conf = TEST_UTIL.getConfiguration();
    Driver driver = new Driver(conf);
    int code = driver.run(new String[] { "-b", "1", tableName, outputPath });
    Assert.assertEquals(0, code);

    // get output file
    FileSystem fs = TEST_UTIL.getTestFileSystem();
    Path path = fs.getHomeDirectory();
    path = new Path(path, outputPath + "/part-r-00000");
    InputStream is = fs.open(path);
    System.out.println("result.content=\n" + IOUtils.toString(is));
    IOUtils.closeQuietly(is);
  }

  @Test
  public void testRun_b4_m3() throws Exception {
    // init test table
    String tableName = "test.vertex-02";
    String outputPath = "/test-02";
    createTestTable(tableName, 3);

    // start test
    Configuration conf = TEST_UTIL.getConfiguration();
    Driver driver = new Driver(conf);
    int code = driver.run(new String[] { "-b", "4", tableName, outputPath });
    Assert.assertEquals(0, code);

    // get output file
    FileSystem fs = TEST_UTIL.getTestFileSystem();
    Path path = fs.getHomeDirectory();
    path = new Path(path, outputPath + "/part-r-00000");
    InputStream is = fs.open(path);
    System.out.println("result.content=\n" + IOUtils.toString(is));
    IOUtils.closeQuietly(is);
  }

  private static void createTestTable(String tableName, int splits) throws Exception, IOException {
    importData(new String[] {
        "-Dimporttsv.columns=HBASE_ROW_KEY,property:name@String,property:lang@String",
        "-Dimporttsv.separator=|" }, tableName, new String[] { "property" },
      "org/trend/hgraph/mapreduce/lib/input/vertex-test.data");
    printTable(tableName);
    Configuration conf = TEST_UTIL.getConfiguration();
    conf.set(HBaseGraphConstants.HBASE_GRAPH_TABLE_VERTEX_NAME_KEY, tableName);

    // split table
    /*
     * if (splits > 0) { // wait for the table settle down Thread.sleep(6000); MiniHBaseCluster
     * cluster = TEST_UTIL.getMiniHBaseCluster(); HBaseAdmin admin = TEST_UTIL.getHBaseAdmin();
     * byte[] table = Bytes.toBytes(tableName); for (int a = 0; a < splits; a++) { try { // split
     * table admin.split(tableName); } catch (InterruptedException e) {
     * System.err.println("split table failed"); e.printStackTrace(System.err); throw e; }
     * List<HRegion> regions = cluster.getRegions(table); for (HRegion region : regions) { while
     * (!region.isAvailable()) { // wait region online } System.out.println("region:" + region);
     * System.out.println("startKey:" + Arrays.toString(region.getStartKey()));
     * System.out.println("endKey:" + Arrays.toString(region.getEndKey())); } } }
     */
  }

}
