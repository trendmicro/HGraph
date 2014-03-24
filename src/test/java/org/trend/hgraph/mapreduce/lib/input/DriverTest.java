package org.trend.hgraph.mapreduce.lib.input;

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

public class DriverTest extends AbstractHBaseGraphTest {

  @BeforeClass
  public static void setUpBeforeClass() throws Exception {
    AbstractHBaseGraphTest.setUpBeforeClass();
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
    createTestTable(tableName, "00030", "00060");

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
    createTestTable(tableName, "00030", "00060");

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

}
