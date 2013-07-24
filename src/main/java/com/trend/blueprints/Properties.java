/**
 * 
 */
package com.trend.blueprints;

import java.math.BigDecimal;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;

import javax.activation.UnsupportedDataTypeException;

import org.apache.commons.lang.Validate;
import org.apache.commons.lang.builder.ToStringBuilder;
import org.apache.commons.lang.builder.ToStringStyle;
import org.apache.hadoop.hbase.KeyValue;
import org.apache.hadoop.hbase.client.Result;
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
   * Get property by given key.
   * @param key
   * @return
   */
  public Object getProperty(String key) {
    return this.keyValueMap.get(key);
  }
  
  /**
   * Get property keys.
   * @return a set of keys
   */
  public Set<String> getPropertyKeys() {
    return this.keyValueMap.keySet();
  }
  
  /**
   * Remove property by given key.
   * @param key
   * @return old property
   */
  public Object removeProperty(String key) {
    if(null == key) return null;
    Object oldValue = null;
    oldValue = this.keyValueMap.remove(key);
    this.keyValueTypeMap.remove(key);
    return oldValue;
  }
  
  /**
   * Set new property
   * @param key
   * @param value
   * @return old property
   * @throws UnsupportedDataTypeException 
   */
  public Object setProperty(String key, Object value) throws UnsupportedDataTypeException {
    Validate.notEmpty(key, "key shall always not be empty or null");
    Validate.notNull(value, "value shall always not be null");
    Object oldValue = this.removeProperty(key);
    @SuppressWarnings("rawtypes")
    Pair<Class, Object> pair = keyValueToPair(key, value, new TypeClassPairStrategy());
    this.keyValueTypeMap.put(key, pair.key);
    return oldValue;
  }
  
  /* (non-Javadoc)
   * @see java.lang.Object#toString()
   */
  @Override
  public String toString() {
    return new ToStringBuilder(this, ToStringStyle.MULTI_LINE_STYLE).
        append("keyValueMap", keyValueMap).
        append("keyValueTypeMap", keyValueTypeMap).
        toString();
  }

  static interface PairStrategy<K, V> {
    Pair<K, V> getStringPair(String key, String value);
    Pair<K, V> getIntPair(String key, Integer value);
    Pair<K, V> getLongPair(String key, Long value);
    Pair<K, V> getShortPair(String key, Short value);
    Pair<K, V> getFloatPair(String key, Float value);
    Pair<K, V> getDoublePair(String key, Double value);
    Pair<K, V> getBooleanPair(String key, Boolean value);
    Pair<K, V> getBigDecimalPair(String key, BigDecimal value);
  }
  
  @SuppressWarnings("rawtypes")
  static class TypeClassPairStrategy implements PairStrategy<Class, Object> {
    
    @Override
    public Pair<Class, Object> getStringPair(String key, String value) {
      Pair<Class, Object> pair = new Pair<Class, Object>(String.class, value);
      return pair;
    }

    @Override
    public Pair<Class, Object> getIntPair(String key, Integer value) {
      Pair<Class, Object> pair = new Pair<Class, Object>(Integer.class, value);
      return pair;
    }

    @Override
    public Pair<Class, Object> getLongPair(String key, Long value) {
      Pair<Class, Object> pair = new Pair<Class, Object>(Long.class, value);
      return pair;
    }

    @Override
    public Pair<Class, Object> getShortPair(String key, Short value) {
      Pair<Class, Object> pair = new Pair<Class, Object>(Short.class, value);
      return pair;
    }

    @Override
    public Pair<Class, Object> getFloatPair(String key, Float value) {
      Pair<Class, Object> pair = new Pair<Class, Object>(Float.class, value);
      return pair;
    }

    @Override
    public Pair<Class, Object> getDoublePair(String key, Double value) {
      Pair<Class, Object> pair = new Pair<Class, Object>(Double.class, value);
      return pair;
    }

    @Override
    public Pair<Class, Object> getBooleanPair(String key, Boolean value) {
      Pair<Class, Object> pair = new Pair<Class, Object>(Boolean.class, value);
      return pair;
    }

    @Override
    public Pair<Class, Object> getBigDecimalPair(String key, BigDecimal value) {
      Pair<Class, Object> pair = new Pair<Class, Object>(BigDecimal.class, value);
      return pair;
    }
    
  }
  
  static class KeyValueToBytesPairStrategy implements PairStrategy<byte[], byte[]> {

    @Override
    public Pair<byte[], byte[]> getStringPair(String key, String value) {
      byte[] keyBytes = null;
      byte[] valueBytes = null;
      keyBytes = Bytes.toBytes(key + HBaseGraphConstants.HBASE_GRAPH_TABLE_COLFAM_PROPERTY_NAME_DELIMITER + 
          String.class.getSimpleName());
      valueBytes = Bytes.toBytes(value);
      return new Pair<byte[], byte[]>(keyBytes, valueBytes);
    }

    @Override
    public Pair<byte[], byte[]> getIntPair(String key, Integer value) {
      byte[] keyBytes = null;
      byte[] valueBytes = null;
      keyBytes = Bytes.toBytes(key + HBaseGraphConstants.HBASE_GRAPH_TABLE_COLFAM_PROPERTY_NAME_DELIMITER + 
          Integer.class.getSimpleName());
      valueBytes = Bytes.toBytes(value);
      return new Pair<byte[], byte[]>(keyBytes, valueBytes);
    }

    @Override
    public Pair<byte[], byte[]> getLongPair(String key, Long value) {
      byte[] keyBytes = null;
      byte[] valueBytes = null;
      keyBytes = Bytes.toBytes(key + HBaseGraphConstants.HBASE_GRAPH_TABLE_COLFAM_PROPERTY_NAME_DELIMITER + 
          Long.class.getSimpleName());
      valueBytes = Bytes.toBytes(value);
      return new Pair<byte[], byte[]>(keyBytes, valueBytes);
    }

    @Override
    public Pair<byte[], byte[]> getShortPair(String key, Short value) {
      byte[] keyBytes = null;
      byte[] valueBytes = null;
      keyBytes = Bytes.toBytes(key + HBaseGraphConstants.HBASE_GRAPH_TABLE_COLFAM_PROPERTY_NAME_DELIMITER + 
          Short.class.getSimpleName());
      valueBytes = Bytes.toBytes(value);
      return new Pair<byte[], byte[]>(keyBytes, valueBytes);
    }

    @Override
    public Pair<byte[], byte[]> getFloatPair(String key, Float value) {
      byte[] keyBytes = null;
      byte[] valueBytes = null;
      keyBytes = Bytes.toBytes(key + HBaseGraphConstants.HBASE_GRAPH_TABLE_COLFAM_PROPERTY_NAME_DELIMITER + 
          Float.class.getSimpleName());
      valueBytes = Bytes.toBytes(value);
      return new Pair<byte[], byte[]>(keyBytes, valueBytes);
    }

    @Override
    public Pair<byte[], byte[]> getDoublePair(String key, Double value) {
      byte[] keyBytes = null;
      byte[] valueBytes = null;
      keyBytes = Bytes.toBytes(key + HBaseGraphConstants.HBASE_GRAPH_TABLE_COLFAM_PROPERTY_NAME_DELIMITER + 
          Double.class.getSimpleName());
      valueBytes = Bytes.toBytes(value);
      return new Pair<byte[], byte[]>(keyBytes, valueBytes);
    }

    @Override
    public Pair<byte[], byte[]> getBooleanPair(String key, Boolean value) {
      byte[] keyBytes = null;
      byte[] valueBytes = null;
      keyBytes = Bytes.toBytes(key + HBaseGraphConstants.HBASE_GRAPH_TABLE_COLFAM_PROPERTY_NAME_DELIMITER + 
          Boolean.class.getSimpleName());
      valueBytes = Bytes.toBytes(value);
      return new Pair<byte[], byte[]>(keyBytes, valueBytes);
    }

    @Override
    public Pair<byte[], byte[]> getBigDecimalPair(String key, BigDecimal value) {
      byte[] keyBytes = null;
      byte[] valueBytes = null;
      keyBytes = Bytes.toBytes(key + HBaseGraphConstants.HBASE_GRAPH_TABLE_COLFAM_PROPERTY_NAME_DELIMITER + 
          BigDecimal.class.getSimpleName());
      valueBytes = Bytes.toBytes(value);
      return new Pair<byte[], byte[]>(keyBytes, valueBytes);
    }

    
  }
  
  /**
   * transfer key and value to byte array.
   * @param key
   * @param value
   * @return a <code>Pair</code> holding key value byte array
   * @throws UnsupportedDataTypeException
   */
  public static Pair<byte[], byte[]> keyValueToBytes(String key, Object value) throws UnsupportedDataTypeException {
    Pair<byte[], byte[]> pair = keyValueToPair(key, value, new KeyValueToBytesPairStrategy());
    return pair;
  }
  
  /**
   * transfer value to <code>K, V</code> generic types, based on its <code>PairStrategy</code> object.
   * @param key
   * @param value
   * @param strategy a <code>PairStrategy</code> object
   * @return a <code>Pair</code> with generic types
   * @throws UnsupportedDataTypeException
   */
  static<K, V> Pair<K, V> keyValueToPair(String key, Object value, PairStrategy<K, V> strategy) 
      throws UnsupportedDataTypeException {
    if(null == key || null == value) return null;
    Validate.notNull(strategy, "strategy shall always not be null");
    
    Pair<K, V> pair = null;
    if(value instanceof String) {
      pair = strategy.getStringPair(key, value.toString());
    } else if(value instanceof Integer) {
      pair = strategy.getIntPair(key, (Integer) value);
    } else if(value instanceof Long) {
      pair = strategy.getLongPair(key, (Long) value);
    } else if(value instanceof Float) {
      pair = strategy.getFloatPair(key, (Float) value);
    } else if(value instanceof Double) {
      pair = strategy.getDoublePair(key, (Double) value);
    } else if(value instanceof Boolean) {
      pair = strategy.getBooleanPair(key, (Boolean) value);
    } else if(value instanceof Short) {
      pair = strategy.getShortPair(key, (Short) value);
    } else if(value instanceof BigDecimal) {
      pair = strategy.getBigDecimalPair(key, (BigDecimal) value);
    } else {
      throw new UnsupportedDataTypeException("Not support data type for value:" + value);
    }
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
  
  /**
   * add a property.
   * @param r
   * @return
   * @throws UnsupportedDataTypeException
   */
  protected Object addProperty(Result r) throws UnsupportedDataTypeException {
    byte[] value = null;
    Object newValue = null;
    Object oldValue = null;
    String keyStr = null;
    String typeStr = null;
    @SuppressWarnings("rawtypes")
    Class type = null;
    int delIdx = 0;
    List<KeyValue> kvs = r.list();
    for(KeyValue kv : kvs) {
      keyStr = Bytes.toString(kv.getQualifier());
      delIdx = keyStr.indexOf(HBaseGraphConstants.HBASE_GRAPH_TABLE_COLFAM_PROPERTY_NAME_DELIMITER);
      typeStr = keyStr.substring(delIdx + 1, keyStr.length());
      keyStr = keyStr.substring(0, delIdx);
      value = kv.getValue();
      if(String.class.getSimpleName().equals(typeStr)) {
        type = String.class;
        newValue = Bytes.toString(value);
      } else if(Integer.class.getSimpleName().equals(typeStr)) {
        type = Integer.class;
        newValue = Bytes.toInt(value);
      } else if(Long.class.getSimpleName().equals(typeStr)) {
        type = Long.class;
        newValue = Bytes.toLong(value);
      } else if(Float.class.getSimpleName().equals(typeStr)) {
        type = Float.class;
        newValue = Bytes.toFloat(value);
      } else if(Double.class.getSimpleName().equals(typeStr)) {
        type = Double.class;
        newValue = Bytes.toDouble(value);
      } else if(Short.class.getSimpleName().equals(typeStr)) {
        type = Short.class;
        newValue = Bytes.toShort(value);
      } else if(Boolean.class.getSimpleName().equals(typeStr)) {
        type = Boolean.class;
        newValue = Bytes.toBoolean(value);
      } else if(BigDecimal.class.getSimpleName().equals(typeStr)) {
        type = BigDecimal.class;
        newValue = Bytes.toBigDecimal(value);
      } else {
        throw new UnsupportedDataTypeException("Not support data type for value:" + value);
      }
      
      oldValue = this.addProperty(keyStr, type, newValue);
      return oldValue;
    }
    
    return oldValue;
  }
  
  

}
