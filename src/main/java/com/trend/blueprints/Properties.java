/**
 * 
 */
package com.trend.blueprints;

import java.math.BigDecimal;
import java.util.HashMap;
import java.util.Map;

import javax.activation.UnsupportedDataTypeException;

import org.apache.commons.lang.Validate;
import org.apache.hadoop.hbase.util.Bytes;

/**
 * @author scott_miao
 *
 */
public class Properties {
  
  private Map<String, Object> keyValueMap = new HashMap<String, Object>();
  
  @SuppressWarnings("rawtypes")
  private Map<String, Class> keyValueTypeMap = new HashMap<String, Class>();
  
  /**
   * Get value by given key.
   * @param key
   * @return
   */
  public Object getValue(String key) {
    return this.keyValueMap.get(key);
  }
  
  /**
   * transfer value to bytes, based on its data type.
   * @param value
   * @return a <code>Pair</code> for key and value byte array
   * @throws UnsupportedDataTypeException
   */
  public static Pair<byte[], byte[]> keyValueToBytes(String key, Object value) throws UnsupportedDataTypeException {
    if(null == key || null == value) return null;
    byte[] keyBytes = null;
    byte[] valueBytes = null;
    Pair<byte[], byte[]> pair = null;
    if(value instanceof String) {
      keyBytes = Bytes.toBytes(key + HBaseGraphConstants.HBASE_GRAPH_TABLE_COLFAM_PROPERTY_NAME_DELIMITER + 
          String.class.getSimpleName());
      valueBytes = Bytes.toBytes(value.toString());
    } else if(value instanceof Integer) {
      keyBytes = Bytes.toBytes(key + HBaseGraphConstants.HBASE_GRAPH_TABLE_COLFAM_PROPERTY_NAME_DELIMITER + 
          Integer.class.getSimpleName());
      valueBytes = Bytes.toBytes((Integer) value);
    } else if(value instanceof Long) {
      keyBytes = Bytes.toBytes(key + HBaseGraphConstants.HBASE_GRAPH_TABLE_COLFAM_PROPERTY_NAME_DELIMITER + 
          Long.class.getSimpleName());
      valueBytes = Bytes.toBytes((Long) value);
    } else if(value instanceof Float) {
      keyBytes = Bytes.toBytes(key + HBaseGraphConstants.HBASE_GRAPH_TABLE_COLFAM_PROPERTY_NAME_DELIMITER + 
          Float.class.getSimpleName());
      valueBytes = Bytes.toBytes((Float) value);
    } else if(value instanceof Double) {
      keyBytes = Bytes.toBytes(key + HBaseGraphConstants.HBASE_GRAPH_TABLE_COLFAM_PROPERTY_NAME_DELIMITER + 
          Double.class.getSimpleName());
      valueBytes = Bytes.toBytes((Double) value);
    } else if(value instanceof Boolean) {
      keyBytes = Bytes.toBytes(key + HBaseGraphConstants.HBASE_GRAPH_TABLE_COLFAM_PROPERTY_NAME_DELIMITER + 
          Boolean.class.getSimpleName());
      valueBytes = Bytes.toBytes((Boolean) value);
    } else if(value instanceof Short) {
      keyBytes = Bytes.toBytes(key + HBaseGraphConstants.HBASE_GRAPH_TABLE_COLFAM_PROPERTY_NAME_DELIMITER + 
          Short.class.getSimpleName());
      valueBytes = Bytes.toBytes((Short) value);
    } else if(value instanceof BigDecimal) {
      keyBytes = Bytes.toBytes(key + HBaseGraphConstants.HBASE_GRAPH_TABLE_COLFAM_PROPERTY_NAME_DELIMITER + 
          BigDecimal.class.getSimpleName());
      valueBytes = Bytes.toBytes((BigDecimal) value);
    } else {
      throw new UnsupportedDataTypeException("Not support data type for value:" + value);
    }
    pair = new Pair<byte[], byte[]>(keyBytes, valueBytes);
    return pair;
  }
  
  public static class Pair<K, V> {
    public K key;
    public V value;
    /**
     * @param key
     * @param value
     */
    public Pair(K key, V value) {
      super();
      this.key = key;
      this.value = value;
    }
    
  }
  
  /**
   * add a property.
   * @param key
   * @param type
   * @param value
   * @return old value if any.
   */
  protected Object addProperty(
      String key, @SuppressWarnings("rawtypes") Class type, Object value) {
    Validate.notEmpty(key, "key shall always not be null or empty");
    Validate.notNull(type, "type shall always not be null");
    Validate.notNull(value, "value shall always not be null");
    Object oldValue = null;
    oldValue = this.keyValueMap.put(key, value);
    this.keyValueTypeMap.put(key, type);
    return oldValue;
  }
  
  

}
