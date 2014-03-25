/**
 * 
 */
package org.trend.hgraph.util;

import java.io.File;
import java.io.FileWriter;
import java.io.IOException;
import java.io.Writer;
import java.util.HashSet;
import java.util.LinkedList;
import java.util.Set;

import org.apache.commons.io.IOUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.client.HTable;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.client.ResultScanner;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.filter.KeyOnlyFilter;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;
import org.trend.hgraph.Graph;
import org.trend.hgraph.HBaseGraphConstants;
import org.trend.hgraph.HBaseGraphFactory;
import org.trend.hgraph.Vertex;

import com.tinkerpop.blueprints.Direction;
import com.tinkerpop.blueprints.Edge;

/**
 * @author scott_miao
 */
public class FindCandidateEntities extends Configured implements Tool {

  protected FindCandidateEntities(Configuration conf) {
    super(conf);
  }

  /*
   * (non-Javadoc)
   * @see org.apache.hadoop.util.Tool#run(java.lang.String[])
   */
  @Override
  public int run(String[] args) throws IOException {
    long num = 1L;
    int idx = 0;
    String startRow = null;

    if (null == args || args.length == 0) {
      System.err.println("No argument specified !!");
      printUsage();
      return -1;
    } else {
      String cmd = null;
      while (idx < args.length) {
        cmd = args[idx];
        if (cmd.startsWith("-")) {
          if ("-n".equals(cmd)) {
            idx++;
            cmd = args[idx];
            try {
              num = Long.parseLong(cmd);
              if (num < 1L) {
                System.err.println("num:" + num + " shall not samll than 1");
                printUsage();
                return -1;
              }
            } catch (NumberFormatException e) {
              System.err.println("Parse num from argument:" + cmd + " failed");
              System.err.println("argument:" + cmd + " shall be a numeric value");
              printUsage();
              return -1;
            }
          } else if ("-s".equals(cmd)) {
            idx++;
            startRow = args[idx];
          } else {
            System.err.println("Not recognized argument:" + cmd + " !!");
            printUsage();
            return -1;
          }
        } else {
          if (idx != args.length - 4) {
            System.err.println("The argument combination is wrong !!");
            printUsage();
            return -1;
          }
          break;
        }
        idx++;
      }
    }
    String vertexTableName = args[idx];
    String edgeTableName = args[idx + 1];
    String vertexOutputFile = args[idx + 2];
    String edgeOutputFile = args[idx + 3];

    // check props
    if ((null == vertexTableName || "".equals(vertexTableName))
        && (null == edgeTableName || "".equals(edgeTableName))
        && (null == vertexOutputFile || "".equals(vertexOutputFile))
        && (null == edgeOutputFile || "".equals(edgeOutputFile))) {
      System.err.println("one of the must arguments is null or empty string");
      printUsage();
      return -1;
    }

    if (isFileExists(vertexOutputFile) || isFileExists(edgeOutputFile)) {
      printUsage();
      return -1;
    }

    // intial props
    Configuration conf = this.getConf();
    conf.set(HBaseGraphConstants.HBASE_GRAPH_TABLE_VERTEX_NAME_KEY, vertexTableName);
    conf.set(HBaseGraphConstants.HBASE_GRAPH_TABLE_EDGE_NAME_KEY, edgeTableName);

    // get vertices from hbase
    HTable table = null;
    Scan scan = new Scan();
    scan.setFilter(new KeyOnlyFilter());
    if (null != startRow) {
      scan.setStartRow(Bytes.toBytes(startRow));
    }
    ResultScanner rs = null;

    Graph graph = HBaseGraphFactory.open(conf);
    String rowkey = null;

    try {
      System.out.print("start to find candidates");
      table = new HTable(conf, vertexTableName);
      rs = table.getScanner(scan);
      for (Result r : rs) {
        System.out.print(".");
        rowkey = Bytes.toString(r.getRow());
        if (isTarget(graph, rowkey, num)) {
          printOutTargets(graph, rowkey, num, vertexOutputFile, edgeOutputFile);
          break;
        }
      }
    } catch (IOException e) {
      System.err.println("get vertex info failed !!");
      e.printStackTrace();
      throw e;
    } finally {
      if (null != table) {
        table.close();
      }
      if (null != graph) {
        graph.shutdown();
      }
    }
    return 0;
  }

  static boolean isFileExists(String fileName) {
    File file = new File(fileName);
    if (file.exists()) {
      System.err.println("file:" + fileName
          + " already exists, pls change to another filepath");
      return true;
    }
    return false;
  }

  private static void printOutTargets(Graph g, String id, long mn, String vf, String ef)
      throws IOException {
    Writer vw = null;
    Writer ew = null;

    try {
      vw = new FileWriter(vf, true);
      ew = new FileWriter(ef, true);

      final Writer fvw = vw;
      final Writer few = ew;
      doBreadthFirstSearch(g, id, mn, new SearchStrategy() {

        @Override
        public void processV(Vertex v) throws IOException {
          String id = v.getId() + "";
          try {
            fvw.append(id + "\n");
          } catch (IOException e) {
            String msg = "write vertex id to file failed, id:" + id;
            System.err.println(msg);
            throw new IOException(msg, e);
          }
        }

        @Override
        public void processE(Edge e) throws IOException {
          String id = e.getId() + "";
          try {
            few.append(id + "\n");
          } catch (IOException ex) {
            String msg = "write edge id to file failed, id:" + id;
            System.err.println(msg);
            throw new IOException(msg, ex);
          }
        }
      });
    } finally {
      IOUtils.closeQuietly(vw);
      IOUtils.closeQuietly(ew);
    }
  }

  private static boolean isTarget(Graph g, String id, long mn) {
    boolean yes = false;
    try {
      yes = doBreadthFirstSearch(g, id, mn, new SearchStrategy() {

        @Override
        public void processV(Vertex v) {
          // DO NOTHING
        }

        @Override
        public void processE(Edge e) {
          // DO NOTHING
        }
      });
    } catch (IOException e) {
      String msg = "This should not happens";
      System.err.println(msg);
      throw new IllegalStateException(msg);
    }
    return yes;
  }

  private static boolean doBreadthFirstSearch(Graph g, String id, long mn, SearchStrategy s)
      throws IOException {
    boolean yes = false;
    Vertex v = g.getVertex(id);
    long cn = 0L;
    String hashCode = null;
    Set<String> set = new HashSet<String>();
    if (null != v) {
      LinkedList<Vertex> queue = new LinkedList<Vertex>();
      queue.add(v);
      while (queue.size() > 0) {
        v = queue.removeFirst();
        hashCode = SecurityUtils.encrypt(v.getId() + "", "SHA");
        if (!set.contains(hashCode)) {
          set.add(hashCode);
          cn++;
          s.processV(v);
          if (cn == mn) {
            yes = true;
            break;
          }
          for (Edge e : v.getEdges()) {
            s.processE(e);
            queue.add((Vertex) e.getVertex(Direction.OUT));
          }
        }
      }
    }
    return yes;
  }

  private static interface SearchStrategy {
    void processV(Vertex v) throws IOException;

    void processE(Edge e) throws IOException;
  }

  private static void printUsage() {
    System.err.println("Usage: " + FindCandidateEntities.class.getSimpleName()
        + " [-n <num>] [-s <start-row>] <vertex-table-name> <edge-table-name>"
        + " <vertext-output-file> <edge-output-file>");
  }

  /**
   * @param args
   * @throws Exception
   */
  public static void main(String[] args) throws Exception {
    Configuration conf = HBaseConfiguration.create();
    FindCandidateEntities tool = new FindCandidateEntities(conf);
    int code = 0;
    try {
      code = ToolRunner.run(tool, args);
    } catch (Exception e) {
      e.printStackTrace();
      throw e;
    }
    System.exit(code);
  }

}
