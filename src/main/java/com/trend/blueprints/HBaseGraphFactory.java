/**
 * 
 */
package com.trend.blueprints;

import org.apache.commons.lang.Validate;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.client.HTablePool;

/**
 * A Factory for initializing the <code>Graph</code> object.
 * @author scott_miao
 *
 */
public class HBaseGraphFactory {
  
  private final static int MAX_SIZE = 10;
  
  /**
   * Open a <code>Graph</code>.
   * @param conf <code>HBaseConfiguration</code>
   * @return a <code>Graph</code>
   */
  public static Graph open(Configuration conf) {
    Validate.notNull(conf, "conf shall always not be null");
    HTablePool pool = new HTablePool(conf, MAX_SIZE);
    return new Graph(pool, conf);
  }

}
