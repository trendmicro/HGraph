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
import java.util.concurrent.ThreadFactory;

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

    private boolean isMs;
    private long taskSt;

    private File ipf;
    private FileReader fr;
    private LineIterator lit;

    private File opf;
    private FileWriter fw;

    private Configuration conf;
    private Graph g;
    private long level;
    private StopWatch timer;

    protected Task(File inputFile, File outputPath, Configuration conf, long level, boolean toMs) {
      super();
      this.ipf = inputFile;
      this.opf = outputPath;
      this.conf = conf;
      this.level = level;
      this.isMs = toMs;
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
      LOGGER.debug("HEAD:g.getVertex");
      Vertex v = g.getVertex(id);
      LOGGER.debug("TAIL:g.getVertex");
      LOGGER.debug("HEAD:traverse(v, 1, level)");
      long count = traverse(v, 1, level);
      LOGGER.debug("TAIL:traverse(v, 1, level)");
      timer.stop();
      long st = timer.getStartTime();
      StringBuffer sb = new StringBuffer();
      if (isMs) {
        sb.append((st - taskSt) + ",");
      } else {
        sb.append(DateFormatUtils.format(st, DATE_FORMAT_PATTERN) + ",");
      }
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

      // task start time
      if (isMs) {
        taskSt = System.currentTimeMillis();
      }

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
      if (cl >= ml) {
        return 1;
      }
      // load data from remote
      long count = 1L;
      // v.getPropertyKeys();
      LOGGER.debug("HEAD:for(Edge e: v.getEdges)");
      for (Edge e : v.getEdges()) {
        LOGGER.debug("HEAD:e = v.getEdges[]");
        // load data from remote
        // e.getPropertyKeys();
        LOGGER.debug("HEAD:traverse(e.getVertex, cl + 1, ml)");
        count += traverse((Vertex) e.getVertex(Direction.OUT), cl + 1, ml);
        LOGGER.debug("TAIL:traverse(e.getVertex, cl + 1, ml)");
        LOGGER.debug("TAIL:e = v.getEdges[]");
      }
      LOGGER.debug("TAIL:for(Edge e: v.getEdges)");
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
    long interval = 1000; // ms
    boolean isMs = false;
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
        } else if ("-m".equals(cmd)) {
          isMs = true;
        } else if ("-i".equals(cmd)) {
          a++;
          cmd = args[a];
          try {
            interval = Long.parseLong(cmd);
          } catch (NumberFormatException e) {
            System.err.println("parse number for -i:" + cmd + " failed");
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
    ThreadFactory tf = new DaemonThreadFactory(Executors.defaultThreadFactory());
    ExecutorService pool = Executors.newFixedThreadPool(threads, tf);
    @SuppressWarnings("rawtypes")
    List<Future> fs = new ArrayList<Future>();
    @SuppressWarnings("rawtypes")
    Future f = null;

    for (int a = 0; a < threads; a++) {
      fs.add(pool.submit(new Task(ipf, opp, conf, level, isMs)));
      synchronized (this) {
        wait(interval);
      }
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

  private static class DaemonThreadFactory implements ThreadFactory {
    private ThreadFactory dtf;

    protected DaemonThreadFactory(ThreadFactory dtf) {
      super();
      this.dtf = dtf;
    }

    @Override
    public Thread newThread(Runnable r) {
      Thread t = dtf.newThread(r);
      t.setDaemon(true);
      return t;
    }
  }

  private static final void printUsage() {
    System.err.print(HGraphClientPerformanceTest.class.getSimpleName() + " Usage:");
    System.err
        .println("[-m] [-l <numerric>] [-t <numeric>] [-i <numeric>] <vertex-table> <edge-table> <input-rowkeys-file> <output-path>");
    System.err.println("A simple tool for testing the query performance for both <vertex-table> and <edge-table>.");
    System.err.println("Usually companion with " + GetRandomRowsByRegions.class.getSimpleName());
    System.err.println("  -m: change time format to millisecond from each task start");
    System.err.println("  -l: how many levels to test, default is 2");
    System.err.println("  -t: how many threads to test, default is 100");
    System.err.println("  -i: how long the interval for each thread to start, default is 1000ms");

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
