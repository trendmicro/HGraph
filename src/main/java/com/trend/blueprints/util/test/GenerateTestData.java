/**
 * 
 */
package com.trend.blueprints.util.test;

import java.io.IOException;
import java.math.BigDecimal;
import java.util.ArrayDeque;
import java.util.HashMap;
import java.util.Map;
import java.util.Queue;
import java.util.UUID;

import javax.activation.UnsupportedDataTypeException;

import org.apache.commons.lang.time.StopWatch;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.client.HTable;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.trend.blueprints.HBaseGraphConstants;
import com.trend.blueprints.Properties;
import com.trend.blueprints.Properties.Pair;

/**
 * Generate test data for testing <code>trend-graph-hbase</code>.
 * @author scott_miao
 *
 */
public class GenerateTestData extends Configured implements Tool {
  
  private static Logger LOG = LoggerFactory.getLogger(GenerateTestData.class);
  
  private static final int DEFAULT_VERTEX_COUNT = 1000;
  private static final int DEFAULT_EDGE_COUNT_PER_VERTEX = 10;
  
  
  private static final Map<String, Object> DEFAULT_PROPERTY_KVS;
  static {
    DEFAULT_PROPERTY_KVS = new HashMap<String, Object>();
    
    DEFAULT_PROPERTY_KVS.put("a", "java");
    DEFAULT_PROPERTY_KVS.put("b", "hbase");
    DEFAULT_PROPERTY_KVS.put("c", "graph");
    DEFAULT_PROPERTY_KVS.put("d", 256);
    DEFAULT_PROPERTY_KVS.put("e", 1000000L);
    DEFAULT_PROPERTY_KVS.put("f", 20.235F);
    DEFAULT_PROPERTY_KVS.put("g", 3.1425678D);
    DEFAULT_PROPERTY_KVS.put("h", (short)1);
    DEFAULT_PROPERTY_KVS.put("i", new BigDecimal("100000"));
    DEFAULT_PROPERTY_KVS.put("j", true);
  }
  
  private static final byte[] COLFAM_PROPERTY_NAME = Bytes.toBytes(HBaseGraphConstants.HBASE_GRAPH_TABLE_COLFAM_PROPERTY_NAME);
  
  private static final String[] LABELS = {"know", "create", "fight", "like", "not like", "link"};
  
  private int vertexCount = DEFAULT_VERTEX_COUNT;
  private int edgeCountPerVertex = DEFAULT_EDGE_COUNT_PER_VERTEX;
  
  private String vertexTable = null;
  private String edgeTable = null;
  
  
  /**
   * for test purpose
   */
  private String firstVertex;

  @Override
  public int run(String[] args) throws Exception {
    
    int countIndex = -1;
    // Process command line args
    for (int i = 0; i < args.length; i++) {
      String cmd = args[i];

      if (cmd.startsWith("-")) {
        if (countIndex >= 0) {
          // command line args must be in the form: [opts] [count1
          // [count2 ...]]
          System.err.println("Invalid command line options");
          printUsageAndExit();
        }

        if (cmd.equals("--help") || cmd.equals("-h")) {
          // user asked for help, print the help and quit.
          printUsageAndExit();
        } else if (cmd.equals("-v")) {
          i++;
          
          if (i == args.length) {
            System.err
            .println("-v needs a numeric value argument.");
            printUsageAndExit();
          }
          
          try{
            this.vertexCount = Integer.parseInt(args[i]);
          } catch(NumberFormatException e) {
            System.err.println("-v needs a numeric value argument.");
            printUsageAndExit();
          }
        } else if (cmd.equals("-e")) {
          i++;

          if (i == args.length) {
            System.err.println("-e needs a numeric value argument.");
            printUsageAndExit();
          }

          try {
            this.edgeCountPerVertex = Integer.parseInt(args[i]);
          } catch (NumberFormatException e) {
            System.err.println("-e needs a numeric value argument.");
            printUsageAndExit();
          }
        } 
      } else if (countIndex < 0) {
        // keep track of first count specified by the user
        countIndex = i;
      }
    }
    
    if(args.length != (countIndex + 2)) {
      System.err.println("the last two option must be <vertex-tablename> and <edge-tablename>");
      printUsageAndExit();
    }
    
    this.vertexTable = args[countIndex];
    this.edgeTable = args[countIndex + 1];
    
    this.doGenerateTestData();
    
    return 0;
  }
  
  private void doGenerateTestData() throws IOException {
    HTable vertexTable = null;
    HTable edgeTable = null;
    Put put = null;
    long vIdx = 0;
    byte[] parentVertexKey = null;
    StopWatch timer = new StopWatch();
    timer.start();
    try {
      vertexTable = new HTable(this.getConf(), this.vertexTable);
      vertexTable.setAutoFlush(false);
      edgeTable = new HTable(this.getConf(), this.edgeTable);
      edgeTable.setAutoFlush(false);
      
      Queue<byte[]> parentVertexKeysQueue = new ArrayDeque<byte[]>();
      for(int rowCount = 0; rowCount < this.vertexCount; rowCount++) {
        put = generateVertexPut();
        vertexTable.put(put);
        parentVertexKeysQueue.offer(put.getRow());
        
        if(rowCount > 0) {
          vIdx = rowCount % this.edgeCountPerVertex;
          if(vIdx == 0) {
            parentVertexKey = parentVertexKeysQueue.poll();
          } else {
            parentVertexKey = parentVertexKeysQueue.peek();
          }
          
          put = generateEdgePut(rowCount, parentVertexKey, put.getRow());
          edgeTable.put(put);
        } else {
          this.firstVertex = Bytes.toString(put.getRow());
        }
      }
      vertexTable.flushCommits();
      edgeTable.flushCommits();
    } catch (IOException e) {
      LOG.error("doGenerateTestData failed", e);
      throw e;
    } finally {
      if(null != vertexTable) vertexTable.close();
      if(null != edgeTable) edgeTable.close();
      timer.stop();
      LOG.info("Time elapsed:" + timer.toString() + " for pushing " + 
          this.vertexCount + " vertices test data to HBase");
      LOG.info("first vertex id:" + this.firstVertex);
    }
  }
  
  private static Put generateVertexPut() {
    Put put = null;
    String rowKey = UUID.randomUUID().toString();
    put = new Put(Bytes.toBytes(rowKey));
    generateProperties(put);
    return put;
  }
  
  private static Put generateEdgePut(int rowCount, byte[] parentVertexKey, byte[] vertexKey) {
    String label = null;
    Put put = null;
    String rowKey = null;
    
    //get label randomly
    label = LABELS[rowCount % LABELS.length];
    
    rowKey = Bytes.toString(parentVertexKey) + HBaseGraphConstants.HBASE_GRAPH_TABLE_EDGE_DELIMITER_1 + 
        label + HBaseGraphConstants.HBASE_GRAPH_TABLE_EDGE_DELIMITER_2 + Bytes.toString(vertexKey);
    put = new Put(Bytes.toBytes(rowKey));
    generateProperties(put);
    return put;
  }
  
  private static void generateProperties(Put put) {
    Pair<byte[], byte[]> pair = null;
    for(Map.Entry<String, Object> entry : DEFAULT_PROPERTY_KVS.entrySet()) {
      try {
        pair = Properties.keyValueToBytes(entry.getKey(), entry.getValue());
        put.add(COLFAM_PROPERTY_NAME, pair.key, pair.value);
      } catch (UnsupportedDataTypeException e) {
        LOG.error("generate property pair failed", e);
        throw new RuntimeException(e);
      }
    }
  }
  
  private static void printUsageAndExit() {
    System.out.print(GenerateTestData.class.getSimpleName() + " Usage:");
    System.out.print(" [-D genericOption [...]] [-v <vertex-count>] [-e <edge-count-per-vertex>]");
    System.out.println(" <vertex-tablename> <edge-tablename>");
    System.out.println("-h --help print usage");
    System.out.println("-v vertex-count default value: " + DEFAULT_VERTEX_COUNT);
    System.out.println("-e edge-count-per-vertex default value: " + DEFAULT_EDGE_COUNT_PER_VERTEX);
    System.exit(1);
  }


  public static int main(String[] args) throws Exception {
    Configuration conf = HBaseConfiguration.create();
    return ToolRunner.run(conf, new GenerateTestData(), args);
  }

  /**
   * @return the firstVertex
   */
  String getFirstVertex() {
    return firstVertex;
  }
}
