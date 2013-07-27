package com.trend.blueprints;

import static org.junit.Assert.*;

import java.math.BigDecimal;
import java.util.UUID;

import org.junit.After;
import org.junit.AfterClass;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;

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

}
