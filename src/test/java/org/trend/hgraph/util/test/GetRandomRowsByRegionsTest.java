package org.trend.hgraph.util.test;


import java.io.InputStream;

import org.apache.commons.io.IOUtils;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.LocatedFileStatus;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.RemoteIterator;
import org.apache.hadoop.hbase.util.Bytes;
import org.junit.After;
import org.junit.AfterClass;
import org.junit.Assert;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;
import org.trend.hgraph.test.AbstractHBaseMiniClusterTest;

public class GetRandomRowsByRegionsTest extends AbstractHBaseMiniClusterTest {

  private static final String TABLE = "test_table_1";
  private static final String[] CF = { "cf_1" };
  private static final String[] QF = { "qf_1" };

  private static final byte[] B_TABLE = Bytes.toBytes(TABLE);
  private static final byte[][] B_CF = transfer2BytesArray(CF);

  @BeforeClass
  public static void setUpBeforeClass() throws Exception {
    AbstractHBaseMiniClusterTest.setUpBeforeClass();
    loadTestData();
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
  public void test_run_b2t3() throws Exception {
    String outputPath = "/run_b2t3";
    GetRandomRowsByRegions tool = new GetRandomRowsByRegions(TEST_UTIL.getConfiguration());
    int status = tool.run(new String[] { "-b", "2", "-t", "3", TABLE, outputPath });
    Assert.assertEquals(0, status);
    // get content, for manual test purpose
    Path path = new Path(outputPath);
    FileSystem fs = path.getFileSystem(TEST_UTIL.getConfiguration());
    RemoteIterator<LocatedFileStatus> it = fs.listFiles(path, false);
    LocatedFileStatus lfs = null;
    InputStream is = null;
    String fn = null;
    while (it.hasNext()) {
      lfs = it.next();
      fn = lfs.getPath().getName();
      if (fn.startsWith("part-")) {
        System.out.println("content for file:" + fn);
        is = fs.open(lfs.getPath());
        System.out.println(IOUtils.toString(is));
        IOUtils.closeQuietly(is);
      }
    }
  }

  private static void loadTestData() throws Exception {
    importData(new String[] {
        "-Dimporttsv.columns=HBASE_ROW_KEY," + CF[0] + ":" + QF[0],
        "-Dimporttsv.separator=|" }, TABLE, CF,
      "org/trend/hgraph/util/test/vertex-test.data");
    printTable(TABLE);

    splitTable(TABLE, "n08", "n16");
  }

}
