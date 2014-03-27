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
package org.trend.hgraph;

import static org.junit.Assert.assertArrayEquals;
import static org.junit.Assert.assertEquals;

import java.math.BigDecimal;
import java.math.RoundingMode;
import java.util.UUID;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import org.apache.commons.lang.time.StopWatch;
import org.junit.After;
import org.junit.AfterClass;
import org.junit.Assert;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Ignore;
import org.junit.Test;

import cern.colt.Arrays;

public class VariousTest {

  @BeforeClass
  public static void setUpBeforeClass() throws Exception {
  }

  @AfterClass
  public static void tearDownAfterClass() throws Exception {
  }

  @Before
  public void setUp() throws Exception {
  }

  @After
  public void tearDown() throws Exception {
  }

  @Test
  public void testClassName() {
    System.out.println(String.class.getSimpleName());
    System.out.println(Integer.class.getSimpleName());
    System.out.println(Long.class.getSimpleName());
    System.out.println(BigDecimal.class.getSimpleName());
  }
  
  @Test
  public void testSubString() {
    String delimiter = "@";
    String prefix = "abcd";
    String suffix = "efgh";
    String value = prefix + delimiter + suffix;
    
    assertEquals(prefix, value.substring(0, value.indexOf(delimiter)));
    assertEquals(suffix, value.substring(value.indexOf(delimiter) + 1, value.length()));
    
    delimiter = "@@";
    value = prefix + delimiter + suffix;
    assertEquals(prefix, value.substring(0, value.indexOf(delimiter)));
    assertEquals(suffix, value.substring(value.indexOf(delimiter) + delimiter.length(), value.length()));
  }
  
  @Test
  public void testUuid() {
    UUID uuid = UUID.randomUUID();
    System.out.println("UUID=" + uuid);
    System.out.println("UUID.toString()=" + uuid.toString());
  }
  
  @Test @Ignore //for test StopWatch behavior
  public void testStopWatch() throws InterruptedException {
    StopWatch timer = new StopWatch();
    
    // #1
    timer.start();
    Thread.sleep(5000);
    timer.stop();
    
    System.out.println("timer.toString=" + timer.toString());
//    System.out.println("timer.toSplitString=" + timer.toSplitString());
    
    // #2
    timer.reset();
    timer.start();
    Thread.sleep(4000);
    timer.split();
    System.out.println("timer.toSplitString=" + timer.toSplitString());
    Thread.sleep(5000);
    timer.split();
    System.out.println("timer.toSplitString=" + timer.toSplitString());
    timer.unsplit();
    Thread.sleep(6000);
    timer.stop();
    
    System.out.println("timer.toString=" + timer.toString());
    
  }
  @Test
  public void testBigDecimal() {
    int a = 10000;
    float b = 0.2F;
    BigDecimal ba = null;
    BigDecimal bb = null;
    
    ba = new BigDecimal(a);
    bb = new BigDecimal(b);
    
    System.out.println(ba.multiply(bb).intValue());
  }
  
  @Test
  public void testBooleanArrayInitial() {
    boolean[] test = new boolean[5];
    System.out.println(Arrays.toString(test));
  }
  
  @Test
  public void testStringSplit() {
    String str1 = "1,2,3";
    assertArrayEquals(new String[]{"1", "2", "3"}, str1.split(","));
    
    String str2 = "1";
    assertArrayEquals(new String[]{"1"}, str2.split(","));
  }
  @Test
  public void testCopyArray() {
    String[] src = {"a", "b", "c"};
    String[] dest = new String[2];
    System.arraycopy(src, 1, dest, 0, 2);
    assertArrayEquals(new String[]{"b", "c"}, dest);
  }

  @Test
  public void testBigDecimalCompare() {
    BigDecimal a = null;
    BigDecimal b = null;

    System.out.println("***test scale");
    a = new BigDecimal("2.12345");
    System.out.println(a.scale());

    a = new BigDecimal("200.12345");
    System.out.println(a.scale());

    a = new BigDecimal("200.12");
    System.out.println(a.scale());

    a = new BigDecimal("200.12345");
    System.out.println(a);
    a = a.setScale(2, RoundingMode.DOWN);
    System.out.println(a);
    System.out.println(a.scale());

    System.out.println("***test compare #1");
    a = new BigDecimal("500.123456");
    b = new BigDecimal("500.126543");

    System.out.println("a=" + a);
    System.out.println("b=" + b);
    System.out.println("a comapre b =>" + a.compareTo(b));

    a = a.setScale(2, RoundingMode.DOWN);
    b = b.setScale(2, RoundingMode.DOWN);

    System.out.println("a=" + a);
    System.out.println("b=" + b);
    System.out.println("a comapre b =>" + a.compareTo(b));

    System.out.println("***test compare #2");
    a = new BigDecimal("500.1");
    b = new BigDecimal("500.126543");

    System.out.println("a=" + a);
    System.out.println("b=" + b);
    System.out.println("a comapre b =>" + a.compareTo(b));

    a = a.setScale(3, RoundingMode.DOWN);
    b = b.setScale(3, RoundingMode.DOWN);

    System.out.println("a=" + a);
    System.out.println("b=" + b);
    System.out.println("a comapre b =>" + a.compareTo(b));
  }

  @Test
  public void testRegExPattern() {
    String str = null;
    String regex = null;
    Pattern p = null;
    Matcher m = null;

    str =
        "191.254.169.46||IPc986b1a0b0d88d5fb2629f||URL-2-2-3-y-h-r-t-7-.v32c-to-5-8w-0yc-tzl8-h2a-7f-ezc-oxt1-7-8y-0elh-be-3k-d.info||DOMAIN";
    regex = "^.+\\|\\|.+\\|\\|.*";
    p = Pattern.compile(regex);
    m = p.matcher(str);
    Assert.assertTrue(m.matches());
  }

}
