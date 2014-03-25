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
package org.trend.hgraph.test;


import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertTrue;

import java.io.IOException;
import java.io.InputStream;
import java.io.UnsupportedEncodingException;
import java.util.ArrayList;
import java.util.List;

import org.apache.commons.io.IOUtils;
import org.apache.commons.lang.Validate;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hbase.HBaseTestingUtility;
import org.apache.hadoop.hbase.HColumnDescriptor;
import org.apache.hadoop.hbase.HConstants;
import org.apache.hadoop.hbase.HTableDescriptor;
import org.apache.hadoop.hbase.KeyValue;
import org.apache.hadoop.hbase.MiniHBaseCluster;
import org.apache.hadoop.hbase.client.HBaseAdmin;
import org.apache.hadoop.hbase.client.HTable;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.client.ResultScanner;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.mapreduce.ImportTsv;
import org.apache.hadoop.hbase.regionserver.HRegion;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.util.GenericOptionsParser;
import org.junit.After;
import org.junit.AfterClass;
import org.junit.Before;
import org.junit.BeforeClass;

public abstract class AbstractHBaseMiniClusterTest {
	
	protected final static HBaseTestingUtility TEST_UTIL = new HBaseTestingUtility();
	
	@BeforeClass
	public static void setUpBeforeClass() throws Exception {
		TEST_UTIL.startMiniCluster(3);
	}

	@AfterClass
	public static void tearDownAfterClass() throws Exception {
		TEST_UTIL.shutdownMiniCluster();
	}

	@Before
	public void setUp() throws Exception {
	}

	@After
	public void tearDown() throws Exception {
	}
	
	/**
	 * Transfer given strings into a array of <code>byte[]</code>.
	 * @param strings
	 * @return
	 */
	protected static byte[][] transfer2BytesArray(String[] strings) {
		List<byte[]> bytesList = new ArrayList<byte[]>();
		if (null != strings && strings.length > 0) {
			for (int a = 0; a < strings.length; a++) {
				bytesList.add(Bytes.toBytes(strings[a]));
			}
		}

		return bytesList.toArray(new byte[][] {});
	}
	
	/**
	 * Import local file to table.
	 * @param conf
	 * @param args
	 * @param TABLE_NAME
	 * @param INPUT_FILE
	 * @throws IOException
	 * @throws InterruptedException
	 * @throws ClassNotFoundException
	 */
	protected static void importLocalFile2Table(
			final Configuration conf, final String[] args, final String TABLE_NAME, 
			final String INPUT_FILE)
			throws IOException, InterruptedException, ClassNotFoundException {
		Validate.notEmpty(INPUT_FILE, "INPUT_FILE shall not be empty or null");
		
		InputStream ips = ClassLoader.getSystemResourceAsStream(INPUT_FILE);
		assertNotNull(ips);

		FileSystem fs = FileSystem.get(conf);
		FSDataOutputStream op = fs.create(new Path(INPUT_FILE), true);
		IOUtils.write(IOUtils.toString(ips), op, HConstants.UTF8_ENCODING);
		IOUtils.closeQuietly(op);
		IOUtils.closeQuietly(ips);
		
		int length = args.length + 2;
		String[] newArgs = new String[length];
		System.arraycopy(args, 0, newArgs, 0, args.length);
		newArgs[length - 2] = TABLE_NAME;
//		newArgs[length - 1] = INPUT_FILE_PATH + INPUT_FILE;
		newArgs[length - 1] = INPUT_FILE;

		Job job = ImportTsv.createSubmittableJob(conf, newArgs);
		job.waitForCompletion(true);
		assertTrue(job.isSuccessful());
	}
	
	/**
	 * Create table and corresponding columnFamilies.
	 * @param conf
	 * @param tableName
	 * @param columnFamilyNames
	 * @throws Exception
	 */
	protected static void createTable(Configuration conf, byte[] tableName, byte[][] columnFamilyNames) throws Exception {
		HTableDescriptor desc = new HTableDescriptor(tableName);
		for (byte[] family : columnFamilyNames) {
			desc.addFamily(new HColumnDescriptor(family));
		}
		new HBaseAdmin(conf).createTable(desc);
	}

	/**
	 * encoding to UTF-8 string.
	 * @param bytes
	 * @return
	 * @throws UnsupportedEncodingException
	 */
	protected static String toU8Str(byte[] bytes)
			throws UnsupportedEncodingException {
		return new String(bytes, HConstants.UTF8_ENCODING);
	}

	/**
	 * Print table content.
	 * @param tableName
	 * @return return key/value count.
	 * @throws IOException
	 */
	protected static long printTable(String tableName) throws IOException {
		MiniHBaseCluster cluster = TEST_UTIL.getHBaseCluster();
		Configuration conf = cluster.getConfiguration();
		HTable hTable = new HTable(new Configuration(conf), tableName);
		long kvCount = 0;
		
		System.out.println("print content for table:" + Bytes.toString(hTable.getTableName()));
		Scan scan = new Scan();
		hTable.getScanner(scan);
		ResultScanner resScanner = hTable.getScanner(scan);
		KeyValue[] kvs = null;
		
		for (Result res : resScanner) {
			System.out.println("\nrowKey = " + Bytes.toString(res.getRow()));
			kvs = res.raw();
			for(KeyValue kv : kvs) {
				kvCount++;
				System.out.println(kv + ", " + Bytes.toString(kv.getValue()));
			}
		}
		return kvCount;
	}

	/**
	 * import local data to mini-cluster.
	 * @param args arguments for <code>ImportTsv</code> use.
	 * @param tableName <code>HTable</code> name
	 * @param columnFamilies HTable column families
	 * @param inputFileName file name
	 * @throws Exception
	 */
	protected static void importData(String[] args, String tableName, String[] columnFamilies,
			String inputFileName) throws Exception {
				// Cluster
				MiniHBaseCluster cluster = TEST_UTIL.getHBaseCluster();
			
				GenericOptionsParser opts = new GenericOptionsParser(
						cluster.getConfiguration(), args);
				Configuration conf = opts.getConfiguration();
				args = opts.getRemainingArgs();
			
				final byte[][] FAMS = transfer2BytesArray(columnFamilies);
				final byte[] TAB = Bytes.toBytes(tableName);
				
				createTable(conf, TAB, FAMS);
				
				importLocalFile2Table(conf, args, tableName, inputFileName);
			}

  protected static void splitTable(String tableName, String... splits) throws InterruptedException,
      IOException {
        // split table
        if (null != splits && splits.length > 0) {
          // wait for the table settle down
          Thread.sleep(6000);
          MiniHBaseCluster cluster = TEST_UTIL.getMiniHBaseCluster();
          HBaseAdmin admin = TEST_UTIL.getHBaseAdmin();
          byte[] table = Bytes.toBytes(tableName);
          List<HRegion> regions = null;
          for (int a = 0; a < splits.length; a++) {
            try {
              // split
              admin.split(table, Bytes.toBytes(splits[a]));
            } catch (InterruptedException e) {
              System.err.println("split table failed");
              e.printStackTrace(System.err);
              throw e;
            }
            Thread.sleep(6000);
      
            regions = cluster.getRegions(table);
            for (HRegion region : regions) {
              while (!region.isAvailable()) {
                // wait region online
              }
              System.out.println("region:" + region);
              System.out.println("startKey:" + Bytes.toString(region.getStartKey()));
              System.out.println("endKey:" + Bytes.toString(region.getEndKey()));
            }
          }
        }
      }

}
