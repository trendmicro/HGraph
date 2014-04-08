package org.trend.hgraph.mapreduce.lib.input;

import java.io.IOException;
import java.io.InputStream;

import org.apache.commons.io.IOUtils;
import org.apache.commons.io.LineIterator;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.util.Tool;
import org.junit.After;
import org.junit.AfterClass;
import org.junit.Assert;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;

public class CalculateInputSplitMapperTest extends AbstractHBaseGraphTest {

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
  public void testRun_b4() throws IOException, Exception {
    // init test table
    String tableName = "test.vertex-02";
    String outputPath = "/run_b4";
    createTestTable(tableName, "00030", "00060");

    // start test
    Configuration conf = TEST_UTIL.getConfiguration();
    Tool driver = new CalculateInputSplitMapper(conf);
    int code = driver.run(new String[] { "-b", "4", tableName, outputPath });
    Assert.assertEquals(0, code);

    // get test results
    Path path = new Path(outputPath);
    FileSystem fs = path.getFileSystem(conf);

    // FileStatus[] files = fs.listStatus(path);
    // for (int a = 0; a < files.length; a++) {
    // System.out.println(files[a].getPath());
    // }
    InputStream is = fs.open(new Path(path, "part-r-00000"));
    LineIterator it = IOUtils.lineIterator(is, "UTF-8");
    System.out.println("print out test results");
    while (it.hasNext()) {
      System.out.println(it.next());
    }
    LineIterator.closeQuietly(it);
    IOUtils.closeQuietly(is);
  }

}
