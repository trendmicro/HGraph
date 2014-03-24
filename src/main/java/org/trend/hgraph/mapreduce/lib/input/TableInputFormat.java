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

import java.io.IOException;
import java.io.InputStream;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;

import org.apache.commons.io.IOUtils;
import org.apache.commons.io.LineIterator;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hbase.HConstants;
import org.apache.hadoop.hbase.HRegionInfo;
import org.apache.hadoop.hbase.HRegionLocation;
import org.apache.hadoop.hbase.client.HTable;
import org.apache.hadoop.hbase.mapreduce.TableSplit;
import org.apache.hadoop.hbase.util.Addressing;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.mapreduce.InputSplit;
import org.apache.hadoop.mapreduce.JobContext;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;



/**
 * A customed <code>InputFormat</code> for generating multiple <code>Mapper</code>s for one HTable
 * region.
 * @author scott_miao
 * @see CalculateInputSplitMapper
 * @see CalculateInputSplitReducer
 */
public class TableInputFormat extends org.apache.hadoop.hbase.mapreduce.TableInputFormat {
  
  private static Logger LOGGER = LoggerFactory.getLogger(TableInputFormat.class);

  public static final String INPUT_SPLIT_PAIRS_INPUT_PATH = "hgraph.mapreduce.lib.input.splits.path";
  private String inputPath;

  @Override
  public List<InputSplit> getSplits(JobContext context) throws IOException {
    LinkedList<InputSplit> newSplits = new LinkedList<InputSplit>();
    FileSystem fs = null;
    InputStream is = null;
    LineIterator it = null;
    try {
      List<InputSplit> targets = super.getSplits(context);
      Map<String, List<TableSplit>> targetMap = createTargetMap(targets);

      HTable table = getHTable();
      byte[] tableName = table.getTableName();

      fs = FileSystem.get(getConf());
      Path path = new Path(inputPath);
      is = fs.open(path);
      it = IOUtils.lineIterator(is, "UTF-8");
      String line = null;
      String[] lineSplits = null;
      String regionName = null;
      byte[] startKey = null;
      byte[] endKey = null;
      byte[] regionStartKey = null;
      byte[] regionEndKey = null;
      boolean nextRegion = false;

      HRegionLocation regionlocation = null;
      String location = null;
      HRegionInfo regionInfo = null;
      List<TableSplit> splits = null;
      TableSplit split = null;
      while (it.hasNext()) {
        if (!nextRegion) {
          line = it.nextLine();
        } else {
          nextRegion = false;
        }
        if (null == line || "".equals(line)) {
          String msg = "skip a invalid line";
          LOGGER.error(msg);
          throw new IllegalStateException(msg);
        }

        lineSplits = getLineSplits(line);
        regionName = lineSplits[0];
        startKey = Bytes.toBytes(lineSplits[1]);
        endKey = Bytes.toBytes(lineSplits[2]);
        LOGGER.info("start to process region:" + regionName);
        regionlocation = table.getRegionLocation(startKey, false);
        if (null == regionlocation) {
          String msg = "can not find location for region:" + regionName + " !!";
          LOGGER.error(msg);
          throw new IllegalStateException(msg);
        }

        regionInfo = regionlocation.getRegionInfo();
        location = regionlocation.getHostnamePort().split(Addressing.HOSTNAME_PORT_SEPARATOR)[0];
        if (!targetMap.containsKey(location)) {
          String msg = "regionLocation:" + regionlocation + " not found in TableSplits !!";
          LOGGER.error(msg);
          throw new IllegalStateException(msg);
        }

        regionStartKey = regionInfo.getStartKey();
        regionEndKey = regionInfo.getEndKey();

        splits = targetMap.get(location);
        LOGGER.info("splits.size is " + splits.size());
        for (int a = 0; a < splits.size(); a++) {
          split = splits.get(a);
          if (Arrays.equals(split.getStartRow(), regionStartKey)
              && Arrays.equals(split.getEndRow(), regionEndKey)) {
            splits.remove(a);

            if (Arrays.equals(HConstants.EMPTY_BYTE_ARRAY, regionStartKey)) {
              startKey = regionStartKey;
            }

            split = new TableSplit(tableName, startKey, endKey, location);
            newSplits.add(split);

            if (it.hasNext()) {
              while (it.hasNext()) {
                line = it.nextLine();
                lineSplits = getLineSplits(line);
                // keep doing due to it may be in same region
                if (regionName.equals(lineSplits[0])) {
                  split =
                      new TableSplit(tableName, Bytes.toBytes(lineSplits[1]),
                          Bytes.toBytes(lineSplits[2]), location);
                  newSplits.add(split);
                } else {
                  checkAndPatchRegionEndKey(newSplits, regionEndKey);

                  nextRegion = true;
                  break;
                }
              }
            } else {
              checkAndPatchRegionEndKey(newSplits, regionEndKey);
            }
            if (splits.size() == 0) {
              // remove itself if it is empty
              targetMap.remove(location);
            }
            break;
          }
        }
      }

      // check if still any split not processed yet ?
      if (targetMap.size() > 0) {
        String msg =
            "targetMap.size:" + targetMap.size() + " still bigger than 0,"
                + " which means that there still has TableSplit(s) not processed yet !!";
        LOGGER.error(msg);
        LOGGER.error("" + targetMap);
        throw new IllegalStateException(msg);
      }
    } catch (IOException e) {
      LOGGER.error("unknow error occues !!", e);
      throw e;
    } finally {
      if (null != it) {
        LineIterator.closeQuietly(it);
      }
      if (null != is) {
        IOUtils.closeQuietly(is);
      }
      if (null != fs) {
        fs.close();
      }
    }

    return newSplits;
  }

  private void checkAndPatchRegionEndKey(LinkedList<InputSplit> newSplits, byte[] regionEndKey) {
    if (Arrays.equals(HConstants.EMPTY_BYTE_ARRAY, regionEndKey)) {
      TableSplit tmpSplit = (TableSplit) newSplits.removeLast();
      newSplits.add(new TableSplit(tmpSplit.getTableName(), tmpSplit.getStartRow(),
          regionEndKey, tmpSplit.getLocations()[0]));
    }
  }

  private static String[] getLineSplits(String line) {
    String[] lineSplits;
    lineSplits = line.split(CalculateInputSplitReducer.DELIMITER);
    if (lineSplits.length != 3) {
      String msg =
          "lineSplits.length:" + lineSplits.length + " is not equals to 3, lineSplits:"
              + Arrays.toString(lineSplits);
      LOGGER.error(msg);
      throw new IllegalStateException(msg);
    }
    return lineSplits;
  }

  private static Map<String, List<TableSplit>> createTargetMap(List<InputSplit> targets) {
    Map<String, List<TableSplit>> targetMap = new HashMap<String, List<TableSplit>>();
    TableSplit target = null;
    String location = null;
    while (targets.size() > 0) {
      target = (TableSplit) targets.remove(0);
      location = target.getLocations()[0];
      if (!targetMap.containsKey(location)) {
        targetMap.put(location, new ArrayList<TableSplit>());
      }
      targetMap.get(location).add(target);
    }
    return targetMap;
  }

  @Override
  public void setConf(Configuration conf) {
    super.setConf(conf);
    String path = conf.get(INPUT_SPLIT_PAIRS_INPUT_PATH);
    if (null == path || "".equals(path)) {
      throw new IllegalArgumentException(INPUT_SPLIT_PAIRS_INPUT_PATH
          + " shall always not be null or empty");
    }
    inputPath = path;
  }

}
