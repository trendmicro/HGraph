package org.trend.hgraph.util.test;

import java.io.File;
import java.io.FileNotFoundException;
import java.io.FileReader;
import java.io.FileWriter;
import java.io.IOException;
import java.io.InputStream;
import java.util.Collection;

import org.apache.commons.io.FileUtils;
import org.apache.commons.io.IOUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.LocatedFileStatus;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.RemoteIterator;
import org.apache.hadoop.util.Tool;
import org.junit.After;
import org.junit.AfterClass;
import org.junit.Assert;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;
import org.trend.hgraph.test.AbstractHBaseMiniClusterTest;

public class HGraphClientPerformanceTestTest extends AbstractHBaseMiniClusterTest {

  private static final String VERTEX_TABLE = "vertex.test_1";
  private static final String EDGE_TABLE = "edge.test_1";
  private static final String[] CF = { "property" };
  private static final String[] QF = { "d@String" };

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
  public void testRun_l2t10() throws Exception {
    // gen rowkeys file for later test
    Configuration conf = TEST_UTIL.getConfiguration();
    String outputPath = "/run_b2t3";
    Tool tool = new GetRandomRowsByRegions(conf);
    int status = tool.run(new String[] { "-b", "2", "-t", "3", VERTEX_TABLE, outputPath });
    Assert.assertEquals(0, status);

    // merge content
    File tf = mergeResults(conf, outputPath, "rowkeys-1");

    // run test
    File tPath = tf.getParentFile();
    tPath = new File(tPath, "performanceTestResults_" + System.currentTimeMillis());
    FileUtils.forceMkdir(tPath);

    tool = new HGraphClientPerformanceTest(conf);
    status =
        tool.run(new String[] { "-l", "2", "-t", "10", VERTEX_TABLE, EDGE_TABLE,
            tf.getAbsolutePath(), tPath.getAbsolutePath() });
    Assert.assertEquals(0, status);

    // verify test results
    outputTestResults(tPath);
  }

  @Test
  public void testRun_ml2t10() throws Exception {
    // gen rowkeys file for later test
    Configuration conf = TEST_UTIL.getConfiguration();
    String outputPath = "/run_ml2t10";
    Tool tool = new GetRandomRowsByRegions(conf);
    int status = tool.run(new String[] { "-b", "2", "-t", "3", VERTEX_TABLE, outputPath });
    Assert.assertEquals(0, status);

    // merge content
    File tf = mergeResults(conf, outputPath, "rowkeys-2");

    // run test
    File tPath = tf.getParentFile();
    tPath = new File(tPath, "performanceTestResults_" + System.currentTimeMillis());
    FileUtils.forceMkdir(tPath);

    tool = new HGraphClientPerformanceTest(conf);
    status =
        tool.run(new String[] { "-m", "-l", "2", "-t", "10", VERTEX_TABLE, EDGE_TABLE,
            tf.getAbsolutePath(), tPath.getAbsolutePath() });
    Assert.assertEquals(0, status);

    // verify test results
    outputTestResults(tPath);
  }

  private void outputTestResults(File tPath) throws FileNotFoundException, IOException {
    String content;
    Collection<File> csvs = FileUtils.listFiles(tPath, new String[] { "csv" }, false);
    FileReader fr = null;
    for (File csv : csvs) {
      fr = new FileReader(csv);
      content = IOUtils.toString(fr);
      System.out.println("content for file:" + csv);
      System.out.println(content);
    }
    IOUtils.closeQuietly(fr);
  }

  private File mergeResults(Configuration conf, String outputPath, String tmpFileName)
      throws IOException,
      FileNotFoundException {
    Path path = new Path(outputPath);
    FileSystem fs = path.getFileSystem(conf);
    RemoteIterator<LocatedFileStatus> it = fs.listFiles(path, false);
    LocatedFileStatus lfs = null;
    InputStream is = null;
    String fn = null;
    String content = null;
    File tf = File.createTempFile(tmpFileName, null);
    FileWriter tfw = new FileWriter(tf);
    while (it.hasNext()) {
      lfs = it.next();
      fn = lfs.getPath().getName();
      if (fn.startsWith("part-")) {
        System.out.println("content for file:" + fn);
        is = fs.open(lfs.getPath());
        content = IOUtils.toString(is);
        tfw.write(content);
        IOUtils.closeQuietly(is);
      }
    }
    IOUtils.closeQuietly(tfw);
    return tf;
  }

  private static void loadTestData() throws Exception {
    importData(new String[] {
        "-Dimporttsv.columns=HBASE_ROW_KEY," + CF[0] + ":" + QF[0],
        "-Dimporttsv.separator=|" }, VERTEX_TABLE, CF,
      "org/trend/hgraph/util/test/vertex-test.data");
    importData(new String[] {
        "-Dimporttsv.columns=HBASE_ROW_KEY," + CF[0] + ":" + QF[0],
        "-Dimporttsv.separator=|" }, EDGE_TABLE, CF,
      "org/trend/hgraph/util/test/edge-test.data");
    
    printTable(VERTEX_TABLE);
    printTable(EDGE_TABLE);

    splitTable(VERTEX_TABLE, "n08", "n16");
  }
}
