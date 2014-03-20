/**
 * 
 */
package org.trend.hgraph.util;

import org.junit.After;
import org.junit.AfterClass;
import org.junit.Assert;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;

/**
 * @author scott_miao
 *
 */
public class SecurityTest {

  /**
   * @throws java.lang.Exception
   */
  @BeforeClass
  public static void setUpBeforeClass() throws Exception {
  }

  /**
   * @throws java.lang.Exception
   */
  @AfterClass
  public static void tearDownAfterClass() throws Exception {
  }

  /**
   * @throws java.lang.Exception
   */
  @Before
  public void setUp() throws Exception {
  }

  /**
   * @throws java.lang.Exception
   */
  @After
  public void tearDown() throws Exception {
  }

  /**
   * Test method for
   * {@link org.trend.hgraph.util.SecurityUtils#SecurityUtils.encrypt(java.lang.String, java.lang.String)}
   * .
   */
  @Test
  public void testEncrypt() {
    String str_1 =
        "D_ulnAjbcBEAEgAFCAx-HEBGCdmdyB0AWCARdjYS1wdWItNTU3Mjg3NjY0NTk3NzMxOcgBCakCp-y4N6sphj6oAwGYBACqBIwBT9DKhY65JPWl8rbq3ToFpXmxYMvfYFFsyaOxxKwyVC_JWnMbK50cZoDd-hCQNBK0MAdmQNz_Ve1rAx--CDz9rM4wk6YFFIsenZjBjvylEW_SPwzZGzKAaKgHhgpoOWVTH4oSRcgvvqwb9W63kbOKmvUplAJVLXIt2kwUNnnBz8eoTF6spmZekxC5sYOABsD-8tr4-83kdqAGIQ%26num%3D1%26sig%3DAOD64_33Yo0CnVqvYYpwfrc6nUAbWPtEPQ%26client%3Dca-pub-5572876645977319%26adurl%3D&price=UtfhoQAAxr0KpmmqAAAbm-prmvyIxIqoj_ALlA||URL,1395217051645.a57f2e0d77fdcf71ad5153fc8fe481b4.  spn-d-hdd9.sjdc:8130  clk.dxpmedia.com/adx/rendering?hdtid=CjVSXDMyN1wzNDFcMjQxXDAwMFwwMDBcMzA2XDI3NVxuXDI0NmlcMjUycVwwMDBcMDMzXDIzMxIDYWR4GgExIhBiYWFiMjNiMDBmMjE3ZTc3KgM0NTMyBDEzNTk6BDkxNjhCG0NBRVNFUGhUSUk5aHE4eldlQ0lOOUNRUFhlSUi6_9kEUhdhZHhfYmFubmVyX2lmcmFtZV9pbWFnZVgAYhtDQUVTRVBoVElJOWhxOHpXZUNJTjlDUVBYZUk&clickUrl=http://adclick.g.doubleclick.net/aclk%3Fsa%3DL%26ai%3DCIuZCoeHXUr2NA6rTmQWbt4CIB6yI29QDjJD_ulnAjbcBEAEgAFCAx-HEBGCdmdyB0AWCARdjYS1wdWItNTU3Mjg3NjY0NTk3NzMxOcgBCakCp-y4N6sphj6oAwGYBACqBIwBT9DKhY65JPWl8rbq3ToFpXmxYMvfYFFsyaOxxKwyVC_JWnMbK50cZoDd-hCQNBK0MAdmQNz_Ve1rAx--CDz9rM4wk6YFFIsenZjBjvylEW_SPwzZGzKAaKgHhgpoOWVTH4oSRcgvvqwb9W63kbOKmvUplAJVLXIt2kwUNnnBz8eoTF6spmZekxC5sYOABsD-8tr4-83kdqAGIQ%26num%3D1%26sig%3DAOD64_33Yo0CnVqvYYpwfrc6nUAbWPtEPQ%26client%3Dca-pub-5572876645977319%26adurl%3D&price=UtfhoQAAxr0KpmmqAAAbm-prmvyIxIqoj_ALlA||URL  frotalmost.ru/domcheck/?d=adguard.com&t=%D0%98%D0%BD%D1%82%D0%B5%D1%80%D0%BD%D0%B5%D1%82-%D1%84%D0%B8%D0%BB%D1%8C%D1%82%D1%80%20Adguard%20%E2%80%94%20%D0%B7%D0%B0%D1%89%D0%B8%D1%82%D0%B0%20%D0%BD%D0%BE%D0%B2%D0%BE%D0%B3%D0%BE%20%D0%BF%D0%BE%D0%BA%D0%BE%D0%BB%D0%B5%D0%BD%D0%B8%D1%8F%20%D0%B4%D0%BB%D1%8F%20%D0%B1%D0%BB%D0%BE%D0%BA%D0%B8%D1%80%D0%BE%D0%B2%D0%BA%D0%B8%20%D1%80%D0%B5%D0%BA%D0%BB%D0%B0%D0%BC%D1%8B%2C%20%D0%B2%D1%81%D0%BF%D0%BB%D1%8B%D0%B2%D0%B0%D1%8E%D1%89%D0%B8%D1%85%20%D0%BE%D0%BA%D0%BE%D0%BD%20%D0%B8%20%D0%B2%D1%80%D0%B5%D0%B4%D0%BE%D0%BD%D0%BE%D1%81%D0%BD%D1%8B%D1%85%20%D1%81%D0%B0%D0%B9%D1%82%D0%BE%D0%B2&ref=http%3A%2F%2Fyandex.ru%2Fyandsearch%3Ftext%3D%25D0%25BA%25D0%25B0%25D0%25BA%2520%25D0%25B7%25D0%25B0%25D0%25B1%25D0%25BB%25D0%25BE%25D0%25BA%25D0%25B8%25D1%2580%25D0%25BE%25D0%25B2%25D0%25B0%25D1%2582%25D1%258C%2520%25D0%25B2%25D1%2581%25D0%25BF%25D0%25BB%25D1%258B%25D0%25B2%25D0%25B0%25D1%258E%25D1%2589%25D1%2583%25D1%258E%2520%25D1%2580%25D0%25B5%25D0%25BA%25D0%25BB%25D0%25B0%25D0%25BC%25D1%2583%2520%25D0%25B2%2520%25D1%258F%25D0%25BD%25D0%25B4%25D0%25B5%25D0%25BA%25D1%2581%2520%25D0%25B1%25D1%2580%25D0%25B0%25D1%2583%25D0%25B7%25D0%25B5%25D1%2580%25D0%25B5%26clid%3D1955454%26lr%3D213&p=http%3A%2F%2Fadguard.com%2Fru%2Fwelcome.html%3Faid%3D17629%26utm_source%3Dyandex%26utm_medium%3Dcpc%26utm_term%3D%25d0%25b7%25d0%25b0%25d0%25b1%25d0%25bb%25d0%25be%25d0%25ba%25d0%25b8%25d1%2580%25d0%25be%25d0%25b2%25d0%25b0%25d1%2582%25d1%258c%2520%25d1%2580%25d0%25b5%25d0%25ba%25d0%25bb%25d0%25b0%25d0%25bc%25d1%2583%26utm_campaign%3DAdguardYandexDirect%26_openstat%3DZGlyZWN0LnlhbmRleC5ydTszMDQ1MTU0OzI5NTg3NjAzMjt5YW5kZXgucnU6Z3VhcmFudGVl%26yclid%3D5691138858703378541&u=0.6591760301962495";
    String result_1 = null;
    String result_2 = null;

    result_1 = SecurityUtils.encrypt(str_1, "MD5");
    result_2 = SecurityUtils.encrypt(str_1, "MD5");
    Assert.assertEquals(result_1, result_2);
    System.out.println("MD5:" + result_1);

    result_1 = SecurityUtils.encrypt(str_1, "SHA");
    result_2 = SecurityUtils.encrypt(str_1, "SHA");
    Assert.assertEquals(result_1, result_2);
    System.out.println("SHA:" + result_1);
  }

}
