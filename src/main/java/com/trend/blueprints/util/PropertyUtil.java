/**
 * 
 */
package com.trend.blueprints.util;

import java.math.BigDecimal;

import javax.activation.UnsupportedDataTypeException;

import org.apache.hadoop.hbase.util.Bytes;

/**
 * a utility for graph property manipulation.
 * @author scott_miao
 *
 */
public class PropertyUtil {
  
  /**
   * transfer value to bytes, based on its data type.
   * @param value
   * @return byte array
   * @throws UnsupportedDataTypeException
   */
  public static byte[] valueToBytes(Object value) throws UnsupportedDataTypeException {
    if(null == value) return null;
    byte[] bytes = null;
    if(value instanceof String) {
      bytes = Bytes.toBytes(value.toString());
    } else if(value instanceof Integer) {
      bytes = Bytes.toBytes((Integer) value);
    } else if(value instanceof Long) {
      bytes = Bytes.toBytes((Long) value);
    } else if(value instanceof Float) {
      bytes = Bytes.toBytes((Float) value);
    } else if(value instanceof Double) {
      bytes = Bytes.toBytes((Double) value);
    } else if(value instanceof Boolean) {
      bytes = Bytes.toBytes((Boolean) value);
    } else if(value instanceof Short) {
      bytes = Bytes.toBytes((Short) value);
    } else if(value instanceof BigDecimal) {
      bytes = Bytes.toBytes((BigDecimal) value);
    } else {
      throw new UnsupportedDataTypeException("Not support data type for value:" + value);
    }
    return bytes;
  }

}
