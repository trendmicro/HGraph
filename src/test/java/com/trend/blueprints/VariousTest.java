package com.trend.blueprints;

import static org.junit.Assert.*;

import java.math.BigDecimal;
import java.util.UUID;

import org.apache.commons.lang.time.StopWatch;
import org.junit.After;
import org.junit.AfterClass;
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
}
