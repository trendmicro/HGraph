/**
 * 
 */
package org.trend.hgraph.util.test;

import java.io.File;
import java.io.FileNotFoundException;
import java.io.FileReader;
import java.io.FileWriter;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;

import org.apache.commons.io.IOUtils;
import org.apache.commons.io.LineIterator;
import org.apache.commons.lang.time.DateFormatUtils;
import org.apache.commons.lang.time.StopWatch;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.trend.hgraph.Graph;
import org.trend.hgraph.HBaseGraphConstants;
import org.trend.hgraph.HBaseGraphFactory;
import org.trend.hgraph.Vertex;

import com.tinkerpop.blueprints.Direction;
import com.tinkerpop.blueprints.Edge;

/**
 * @author scott_miao
 *
 */
public class HGraphClientPerformanceTest extends Configured implements Tool {

  private static Logger LOGGER = LoggerFactory.getLogger(HGraphClientPerformanceTest.class);

  protected HGraphClientPerformanceTest(Configuration conf) {
    super(conf);
  }

  private static class Task implements Runnable {

    private static final String DATE_FORMAT_PATTERN = "yyyy-MM-dd HH:mm:ss.S";

    private File ipf;
    private FileReader fr;
    private LineIterator lit;

    private File opf;
    private FileWriter fw;

    private Configuration conf;
    private Graph g;
    private long level;
    private StopWatch timer;

    protected Task(File inputFile, File outputPath, Configuration conf, long level) {
      super();
      this.ipf = inputFile;
      this.opf = outputPath;
      this.conf = conf;
      this.level = level;
    }

    @Override
    public void run() {
      while(!Thread.interrupted()) {
        if (null == fr) {
          if (!initial()) {
            LOGGER.error("initial failed");
            return;
          }
        }

        if (lit.hasNext()) {
          if (!doTest()) {
            LOGGER.error("doTest failed");
            return;
          }
        } else {
          try {
            // DO NOTHING
          } finally {
            LOGGER.info(Thread.currentThread().getName() + " finished the test");
            // housekeeping
            LineIterator.closeQuietly(lit);
            IOUtils.closeQuietly(fr);
            IOUtils.closeQuietly(fw);
          }
          return;
        }
      }
    }

    private boolean doTest() {
      String id = lit.next().trim();
      LOGGER.info("test for id:" + id);
      timer.reset();
      timer.start();
      Vertex v = g.getVertex(id);
      long count = traverse(v, 1, level);
      timer.stop();
      long st = timer.getStartTime();
      StringBuffer sb = new StringBuffer();
      sb.append(DateFormatUtils.format(st, DATE_FORMAT_PATTERN) + ",");
      sb.append(id + ",");
      sb.append(count + ",");
      sb.append(timer.getTime() + "\n");
      try {
        LOGGER.info("write tested result for id:" + id);
        fw.write(sb.toString());
        fw.flush();
      } catch (IOException e) {
        LOGGER.error("write test data to file:" + opf + " failed", e);
        return false;
      }
      return true;
    }

    // start to initialize
    private boolean initial() {
      // graph object
      g = HBaseGraphFactory.open(conf);

      // read ids
      try {
        fr = new FileReader(ipf);
      } catch (FileNotFoundException e) {
        LOGGER.error("open file:" + ipf + " for reading, failed", e);
        return false;
      }
      LOGGER.info(Thread.currentThread().getName() + " start the test");
      lit = IOUtils.lineIterator(fr);

      // write test results
      opf = new File(opf, Thread.currentThread().getName() + ".csv");
      try {
        fw = new FileWriter(opf);
      } catch (IOException e) {
        LOGGER.error("open file:" + opf + " for writing, failed", e);
        return false;
      }
      // timer
      timer = new StopWatch();

      return true;
    }

    private static long traverse(Vertex v, long cl, long ml) {
      if (cl > ml) {
        return 0;
      }
      // load data from remote
      long count = 1L;
      // v.getPropertyKeys();
      for (Edge e : v.getEdges()) {
        // load data from remote
        // e.getPropertyKeys();
        count += traverse((Vertex) e.getVertex(Direction.OUT), cl + 1, ml);
      }
      return count;
    }
  }

  /* (non-Javadoc)
   * @see org.apache.hadoop.util.Tool#run(java.lang.String[])
   */
  @Override
  public int run(String[] args) throws Exception {
    if (null == args || args.length < 4) {
      System.err.println("Options must greater than 4");
      printUsage();
      return -1;
    }

    System.out.println("args=" + Arrays.toString(args));
    String cmd = null;
    int mustStartIdx = -1;
    int level = 2;
    int threads = 100;
    for (int a = 0; a < args.length; a++) {
      cmd = args[a];
      if (cmd.startsWith("-")) {
        if (mustStartIdx != -1) {
          System.err.println("must start option order is incorrect");
          printUsage();
          return -1;
        }

        if ("-l".equals(cmd)) {
          a++;
          cmd = args[a];
          try {
            level = Integer.parseInt(cmd);
          } catch (NumberFormatException e) {
            System.err.println("parse number for -l:" + cmd + " failed");
            printUsage();
            return -1;
          }
        } else if ("-t".equals(cmd)) {
          a++;
          cmd = args[a];
          try {
            threads = Integer.parseInt(cmd);
          } catch (NumberFormatException e) {
            System.err.println("parse number for -t:" + cmd + " failed");
            printUsage();
            return -1;
          }
        } else {
          System.err.println("undefined option:" + cmd);
          printUsage();
          return -1;
        }

      } else {
        if (mustStartIdx == -1) {
          mustStartIdx = a;
          break;
        } else {
          System.err.println("must start option order is incorrect");
          printUsage();
          return -1;
        }
      }
    }

    if (mustStartIdx + 4 != args.length) {
      System.err.println("The must option still not satisfied !!");
      printUsage();
      return -1;
    }

    String vt = args[mustStartIdx];
    String et = args[mustStartIdx + 1];
    File ipf = new File(args[mustStartIdx + 2]);
    File opp = new File(args[mustStartIdx + 3]);
    Configuration conf = this.getConf();

    conf.set(HBaseGraphConstants.HBASE_GRAPH_TABLE_VERTEX_NAME_KEY, vt);
    conf.set(HBaseGraphConstants.HBASE_GRAPH_TABLE_EDGE_NAME_KEY, et);

    // run test threads
    ExecutorService pool = Executors.newFixedThreadPool(threads);
    @SuppressWarnings("rawtypes")
    List<Future> fs = new ArrayList<Future>();
    @SuppressWarnings("rawtypes")
    Future f = null;
    for (int a = 0; a < threads; a++) {
      fs.add(pool.submit(new Task(ipf, opp, conf, level)));
    }

    while (fs.size() > 0) {
      f = fs.get(0);
      f.get();
      if (f.isDone()) {
        if (f.isCancelled()) {
          LOGGER.warn("a future:" + f + " was cancelled !!");
        }
        fs.remove(0);
      }
    }

    return 0;
  }

  private static final void printUsage() {
    System.err.print(HGraphClientPerformanceTest.class.getSimpleName() + " Usage:");
    System.err
        .println(" [-l <numerric>] [-t <numeric>] <vertex-table> <edge-table> <input-rowkeys-file> <output-path>");
    System.err.println("  -l: how many levels to test, default is 2");
    System.err.println("  -t: how many threads to test, default is 100");
  }

  /**
   * @param args
   * @throws Exception
   */
  public static void main(String[] args) throws Exception {
    Configuration conf = HBaseConfiguration.create();
    Tool tool = new HGraphClientPerformanceTest(conf);
    int status = ToolRunner.run(tool, args);
    System.exit(status);
  }

}
