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
package org.trend.hgraph.mapreduce.pagerank;

import static org.trend.hgraph.mapreduce.pagerank.CalculatePageRankReducer.pageRankEquals;

import java.io.InputStream;
import java.io.InputStreamReader;
import java.util.ArrayList;
import java.util.List;

import org.apache.commons.io.IOUtils;
import org.apache.commons.io.LineIterator;
import org.apache.commons.lang.builder.EqualsBuilder;
import org.apache.commons.lang.builder.ToStringBuilder;
import org.junit.After;
import org.junit.AfterClass;
import org.junit.Assert;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;
import org.trend.hgraph.HBaseGraphConstants;

/**
 * A local pageRank test for verifying the MR version.
 * @author scott_miao
 */
public class LocalPrTest {

  private static final String TEST_DATA_EDGE_01 = "org/trend/hgraph/mapreduce/pagerank/edge-test-01.data";
  private static final String TEST_DATA_VERTEX_01 = "org/trend/hgraph/mapreduce/pagerank/vertex-test-01.data";
  
  private static final String TEST_DATA_EDGE_02 = "org/trend/hgraph/mapreduce/pagerank/edge-test-02.data";
  private static final String TEST_DATA_VERTEX_02 = "org/trend/hgraph/mapreduce/pagerank/vertex-test-02.data";
  
  /**
   * @throws java.lang.Exception
   */
  @BeforeClass
  public static void setUpBeforeClass() throws Exception {
  }

  /**
   * @throws java.lang.Exception
   */
  @AfterClass
  public static void tearDownAfterClass() throws Exception {
  }

  /**
   * @throws java.lang.Exception
   */
  @Before
  public void setUp() throws Exception {
  }

  /**
   * @throws java.lang.Exception
   */
  @After
  public void tearDown() throws Exception {
  }

  @Test
  public void testData_1() {
    List<V> vs = generateVs(TEST_DATA_VERTEX_01, TEST_DATA_EDGE_01);
    runPr(vs);
  }
  
  @Test
  public void testData_2() {
    List<V> vs = generateVs(TEST_DATA_VERTEX_02, TEST_DATA_EDGE_02);
    runPr(vs);
  }

  private void runPr(List<V> vs) {
    int prChanged = 0;
    int round = 0;
    do {
      round++;
      prChanged = 0;
      // housekeeping
      for (V v : vs)
        v.tprs.clear();

      // as mapper
      double cpr = 0.0D;
      double tpr = 0.0D;
      for (V v : vs) {
        cpr = v.pr;
        tpr = cpr / (double) v.al.size();
        for (V alv : v.al)
          alv.tprs.add(tpr);
      }

      // as reducer
      double opr = 0.0D;
      double pr = 0.0D;
      double npr = 0.0D;
      for (V v : vs) {
        opr = v.pr;
        pr = 0.0D;
        for (double tpr1 : v.tprs) {
          pr = pr + tpr1;
        }
        npr = (0.85D * pr) + ((1.0D - 0.85D) / (double) vs.size());
        if (!pageRankEquals(opr, npr, 3)) prChanged++;
        v.pr = npr;
      }
      System.out.println("round=" + round + ", prChanged=" + prChanged + ", vs=\n" + vs);
    } while (prChanged > 0);
  }

  private static List<V> generateVs(String vertexData, String edgeData) {
    InputStream data =
 DriverTest.class.getClassLoader().getResourceAsStream(vertexData);
    LineIterator it = IOUtils.lineIterator(new InputStreamReader(data));

    // load all Vs
    List<V> vs = new ArrayList<V>();
    String record = null;
    String[] values = null;
    V v = null;
    while (it.hasNext()) {
      record = it.next();
      values = record.split("\\|");
      Assert.assertNotNull(values);
      Assert.assertEquals(2, values.length);
      v = new V();
      v.key = values[0];
      v.pr = Double.parseDouble(values[1]);
      vs.add(v);
    }
    LineIterator.closeQuietly(it);
    IOUtils.closeQuietly(data);

    // build adjacency list
    data = DriverTest.class.getClassLoader().getResourceAsStream(edgeData);
    it = IOUtils.lineIterator(new InputStreamReader(data));
    while (it.hasNext()) {
      record = it.next();
      values = record.split("\\|");
      Assert.assertNotNull(values);
      Assert.assertEquals(2, values.length);

      values = values[0].split(HBaseGraphConstants.HBASE_GRAPH_TABLE_EDGE_DELIMITER_1);
      Assert.assertNotNull(values);
      Assert.assertEquals(3, values.length);
      for (V tv1 : vs) {
        if (tv1.key.equals(values[0])) {
          for (V tv2 : vs) {
            if (tv2.key.equals(values[2])) {
              tv1.al.add(tv2);
              break;
            }
          }
          break;
        }
      }
    }
    LineIterator.closeQuietly(it);
    IOUtils.closeQuietly(data);

    System.out.println("Vs=\n" + vs);
    return vs;
  }

  private static class V {
    String key;
    double pr;
    List<V> al = new ArrayList<V>(); // adjacency list
    List<Double> tprs = new ArrayList<Double>();

    /*
     * (non-Javadoc)
     * @see java.lang.Object#equals(java.lang.Object)
     */
    @Override
    public boolean equals(Object obj) {
      if (obj == null) { return false; }
      if (obj == this) { return true; }
      if (obj.getClass() != getClass()) {
        return false;
      }
      V rhs = (V) obj;
      return new EqualsBuilder().append(key, rhs.key).isEquals();
    }

    /*
     * (non-Javadoc)
     * @see java.lang.Object#toString()
     */
    @Override
    public String toString() {
      List<String> alkey = new ArrayList<String>();
      for (V v : al)
        alkey.add(v.key);

      return new ToStringBuilder(this).append("key", key).append("pr", pr).append("tprs", tprs)
          .append("al", alkey.toArray(new String[] {})).toString();
    }
  }

}
