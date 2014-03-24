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
package org.trend.hgraph.mapreduce.lib.input;

import static org.junit.Assert.assertEquals;

import org.apache.hadoop.hbase.util.Bytes;
import org.junit.After;
import org.junit.AfterClass;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;

/**
 * @author scott_miao
 *
 */
public class TestTableInputFormat {

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
  public void testSplitKeys() {
    // compare empty key
    byte[] key_1 = { 0 };
    byte[] key_2 = { 0, 0, 0, 0, 0, 0 };
    assertEquals(-5, Bytes.compareTo(key_1, key_2));
    assertEquals(5, Bytes.compareTo(key_2, key_1));

    // compare same content but length
    key_1 = new byte[] { 0, 0, 0, 0, 1 };
    key_2 = new byte[] { 0, 0, 0, 0, 1, 0 };
    assertEquals(-1, Bytes.compareTo(key_1, key_2));
    assertEquals(1, Bytes.compareTo(key_2, key_1));

    // compare diff value
    key_1 = new byte[] { 0, 0, 0, 1, 0 };
    assertEquals(1, Bytes.compareTo(key_1, key_2));
    assertEquals(-1, Bytes.compareTo(key_2, key_1));

    key_1 = new byte[] { 1 };
    key_2 = new byte[] { 0, 0, 0, 0, 0, 0 };
    assertEquals(1, Bytes.compareTo(key_1, key_2));
    assertEquals(-1, Bytes.compareTo(key_2, key_1));
  }

}
