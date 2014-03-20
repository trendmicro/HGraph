/**
 * 
 */
package org.trend.hgraph.util;

import java.security.MessageDigest;

/**
 * @author scott_miao
 *
 */
public class SecurityUtils {

  public static String encrypt(String str, String encType) {
    String result = "";
    try {
      MessageDigest md = MessageDigest.getInstance(encType);
      md.update(str.getBytes());
      result = toHexString(md.digest());
    } catch (Exception e) {
      e.printStackTrace();
    }
    return result;
  }

  public static String getSha1(String str) {
    return encrypt(str, "SHA1");
  }

  private static String toHexString(byte[] in) {
    StringBuilder hexString = new StringBuilder();
    for (int i = 0; i < in.length; i++) {
      String hex = Integer.toHexString(0xFF & in[i]);
      if (hex.length() == 1) {
        hexString.append('0');
      }
      hexString.append(hex);
    }
    return hexString.toString();
  }

}
