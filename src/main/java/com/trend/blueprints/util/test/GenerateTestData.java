/**
 * 
 */
package com.trend.blueprints.util.test;

import java.io.IOException;
import java.math.BigDecimal;
import java.util.ArrayDeque;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Queue;
import java.util.Random;
import java.util.UUID;

import javax.activation.UnsupportedDataTypeException;

import org.apache.commons.lang.Validate;
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

import cern.colt.Arrays;

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
  
  private static final int[] DEFAULT_EDGE_COUNT_FOR_VERTICES = {5, 10, 20, 100, 1000};
  private static final float[] DEFAULT_DIST_PERCS = {0.2F, 0.3F, 0.35F, 0.1F, 0.05F};
  
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
  
  private static final Random random;
  static {
    random = new Random(System.currentTimeMillis());
  }
  
  private String vertexTable = null;
  private String edgeTable = null;
  
  private boolean isDistributionMode = false;
  
  private int vertexCount = DEFAULT_VERTEX_COUNT;
  private int edgeCountPerVertex = DEFAULT_EDGE_COUNT_PER_VERTEX;
  
  private int[] edgeCountForVertices = DEFAULT_EDGE_COUNT_FOR_VERTICES;
  
  /**
   * distribution percentages
   */
  private float[] distPercs = DEFAULT_DIST_PERCS;
  
  private long[] vertexCountForDistPercs = null;
  
  /**
   * Is first Vertex of each category ?
   */
  private boolean[] isFirstVertices = null;
  
  /**
   * for test purpose
   */
  private List<String> firstVertices = new ArrayList<String>();

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
        } else if (cmd.equals("-ev")) {
          i++;
          
          if (i == args.length) {
            System.err.println("-ev needs a ',' delimitered numeric value argument.");
            printUsageAndExit();
          }
          
          String[] edgeCountStrs = args[i].split(",");
          int[] edgeCounts = new int[edgeCountStrs.length];
          for(int a = 0; a < edgeCountStrs.length; a++) {
            try {
              edgeCounts[a] = Integer.parseInt(edgeCountStrs[a]);
            } catch(NumberFormatException e) {
              System.err.println("-ev needs a ',' delimitered numeric value argument.");
              printUsageAndExit();
            }
          }
          
          for(int a = 1; a < edgeCounts.length; a++) {
            if(edgeCounts[a - 1] > edgeCounts[a]) {
              System.err.print("the before edgeCount shall always small than after");
              System.err.println(", values:" + Arrays.toString(edgeCounts));
              printUsageAndExit();
            }
          }
          
          this.edgeCountForVertices = edgeCounts;
        } else if (cmd.equals("-p")) {
          i++;
          
          if (i == args.length) {
            System.err.println("-p needs a ',' delimitered decimal value argument.");
            printUsageAndExit();
          }
          
          String[] distPercsStrs = args[i].split(",");
          float[] percs = new float[distPercsStrs.length];
          for(int a = 0; a < distPercsStrs.length; a++) {
            try {
              percs[a] = Float.parseFloat(distPercsStrs[a]);
            } catch(NumberFormatException e) {
              System.err.println("-p needs a ',' delimitered decimal value argument.");
              printUsageAndExit();
            }
          }
          
          float sum = 0F;
          for(float perc : percs) {
            sum += perc;
          }
          if(sum != 1.0F) {
            System.err.println("The total sum must be 1.0, values:" + Arrays.toString(percs));
            printUsageAndExit();
          }
          this.distPercs = percs;
        } else if (cmd.equals("-d")) {
          this.isDistributionMode = true;
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
    
    if(this.isDistributionMode) {
      if(this.edgeCountForVertices.length != this.distPercs.length) {
        System.err.println("The element size between edge-count-for-vertices" + 
            " and distribution-percentages shall be the same");
        System.err.println("edge-count-for-vertices values:" + 
            Arrays.toString(this.edgeCountForVertices) + 
            ", distribution-percentages values:" + Arrays.toString(this.distPercs));
        printUsageAndExit();
      }
      
      this.initialVertexCountForDistPercs();
      this.isFirstVertices = new boolean[this.edgeCountForVertices.length];
      
      LOG.info("-d, -ev:" + Arrays.toString(this.edgeCountForVertices) + ", -p:" +
          Arrays.toString(this.distPercs) + ", vertex-table-name:" + this.vertexTable + 
          ", edge-table-name:" + this.edgeTable);
    } else {
      LOG.info("-v:" + this.vertexCount + ", -e:" + this.edgeCountPerVertex + 
          ", vertex-table-name:" + this.vertexTable + ", edge-table-name:" + this.edgeTable);
    }
    
    this.doGenerateTestData();
    
    return 0;
  }
  
  private void initialVertexCountForDistPercs() {
    this.vertexCountForDistPercs =  new long[this.distPercs.length];
    for(int a = 0; a < this.distPercs.length; a++) {
      if(a + 1 == this.distPercs.length) {
        this.vertexCountForDistPercs[a] = this.vertexCount;
      } else if(a == 0) {
        this.vertexCountForDistPercs[a] = new BigDecimal(this.vertexCount).multiply(
            new BigDecimal((double) this.distPercs[a])).intValue();
      } else {
        this.vertexCountForDistPercs[a] = new BigDecimal(this.vertexCount).multiply(
            new BigDecimal((double) this.distPercs[a])).intValue() + 
            this.vertexCountForDistPercs[a - 1];
      }
    }
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
      int tmpEdgeCountPerVertex = 0;
      int edgeAcctCount = 0;
      Properties.Pair<Integer, Integer> pair = null;
      for(int rowCount = 0; rowCount < this.vertexCount; rowCount++) {
        put = generateVertexPut();
        vertexTable.put(put);
        parentVertexKeysQueue.offer(put.getRow());
        
        if(rowCount > 0) {
          vIdx = rowCount % tmpEdgeCountPerVertex;
          if(vIdx == 0) {
            parentVertexKey = parentVertexKeysQueue.poll();
            
            edgeAcctCount++;
            if(this.isDistributionMode && !this.isFirstVertices[pair.key] && 
                edgeAcctCount == tmpEdgeCountPerVertex) {
              this.addFirstVertex(Bytes.toString(parentVertexKey));
              this.isFirstVertices[pair.key] = true;
            }
            pair = this.determineEdgeCountPerVertex(rowCount);
            tmpEdgeCountPerVertex = pair.value;
            edgeAcctCount = 0;
          } else if(vIdx > 0) {
            edgeAcctCount++;
            parentVertexKey = parentVertexKeysQueue.peek();
          } else {
            throw new RuntimeException("vIdex:" + vIdx + " shall always not small than 0");
          }
          
          put = generateEdgePut(rowCount, parentVertexKey, put.getRow());
          edgeTable.put(put);
        } else {
          pair = this.determineEdgeCountPerVertex(rowCount);
          tmpEdgeCountPerVertex = pair.value;
          if(!this.isDistributionMode)
            this.addFirstVertex(Bytes.toString(put.getRow()));
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
      LOG.info("Time elapsed:" + timer.toString() + ", " + timer.getTime() + 
          " for pushing " + this.vertexCount + " vertices test data to HBase");
      LOG.info("first vertices id:" + this.firstVertices);
    }
  }
  
  /**
   * @param rowCount
   * @return <code>pair.key</code> is idx used in {@link #edgeCountForVertices},<br>
   * <code>pair.value</code> is edgeCountPerVertex
   */
  private Properties.Pair<Integer, Integer> determineEdgeCountPerVertex(int rowCount) {
    int tmpEdgeCountPerVertex = 0;
    int idx = -1;
    int start = 0;
    int end = 0;
    
    if(this.isDistributionMode) {
      for(long count : this.vertexCountForDistPercs) {
        idx++;
        if(count > rowCount) {
          if(idx == 0) {
            start = 1;
          } else {
            start = this.edgeCountForVertices[idx - 1] + 1;
          }
          end = this.edgeCountForVertices[idx] + 1;
          tmpEdgeCountPerVertex = random.nextInt(end - start) + start;
          break;
        }
      }
    } else {
      tmpEdgeCountPerVertex = this.edgeCountPerVertex;
    }
    return new Pair<Integer, Integer>(idx, tmpEdgeCountPerVertex);
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
    System.err.print(GenerateTestData.class.getSimpleName() + " Usage:");
    System.err.print(" [-D genericOption [...]] "); 
    System.err.print(" [-v <vertex-count>] [-e <edge-count-per-vertex>]");
    System.err.print(" [-d [-ev <edge-count-for-vertices>] [-p <distribution-percentages>]]");
    System.err.println(" <vertex-tablename> <edge-tablename>");
    System.err.println("-h --help print usage");
    System.err.println("-v vertex-count, default value: " + DEFAULT_VERTEX_COUNT);
    System.err.println("-e edge-count-per-vertex, default value: " + DEFAULT_EDGE_COUNT_PER_VERTEX);
    System.err.println("-d enable normal-distribution mode");
    System.err.println("-ev edge-count-for-vertices, specify edge" + 
        " count for each category of given vertex, delimitered by ','");
    System.err.println("    it is opposite with edge-count-per-vertex");
    System.err.println("    default vaule:" + Arrays.toString(DEFAULT_EDGE_COUNT_FOR_VERTICES));
    System.err.println("-p distribution-percentages, specify percentages" + 
        " of vertices will have edge count, delimitered by ','");
    System.err.println("    default value:" + Arrays.toString(DEFAULT_DIST_PERCS));
    System.exit(1);
  }


  public static void main(String[] args) throws Exception {
    Configuration conf = HBaseConfiguration.create();
    int returnCode = ToolRunner.run(conf, new GenerateTestData(), args);
    System.exit(returnCode);
  }

  /**
   * @return the firstVertices
   */
  List<String> getFirstVertices() {
    return firstVertices;
  }
  
  void addFirstVertex(String id) {
    Validate.notEmpty(id, "id for firstVertices shall always not be null or empty");
    this.firstVertices.add(id);
  }

}
