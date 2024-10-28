package io.github.octopus.datos.centro.util;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.SQLException;
import org.apache.commons.lang3.StringUtils;

public class JdbcUtil {
  public static Connection getConnection(String url) throws SQLException, ClassNotFoundException {
    return getConnection(url, null, null);
  }

  public static Connection getConnection(String url, String username, String password)
      throws SQLException, ClassNotFoundException {
    return getConnection(url, username, password, null);
  }

  /**
   * 通过ServiceLoader的方式，自动发现Driver
   *
   * @param url
   * @param username
   * @param password
   * @param databaseName
   * @return
   * @throws SQLException
   * @throws ClassNotFoundException
   */
  public static Connection getConnection(
      String url, String username, String password, String databaseName)
      throws SQLException, ClassNotFoundException {
    if (StringUtils.isBlank(url)) {
      throw new NullPointerException("jdbc url cannot be null");
    }

    String jdbcUrl = replaceDatabase(url, databaseName);
    if (StringUtils.isNotBlank(username)) {
      return DriverManager.getConnection(jdbcUrl, username, password);
    }
    return DriverManager.getConnection(url);
  }

  public static String replaceDatabase(String jdbcUrl, String databaseName) {
    if (databaseName == null) {
      return jdbcUrl;
    }
    String[] split = jdbcUrl.split("\\?");
    if (split.length == 1) {
      return replaceDatabaseWithoutParameter(jdbcUrl, databaseName);
    }
    return replaceDatabaseWithoutParameter(split[0], databaseName) + "?" + split[1];
  }

  private static String replaceDatabaseWithoutParameter(String jdbcUrl, String databaseName) {
    int lastIndex = jdbcUrl.lastIndexOf(':');
    char[] chars = jdbcUrl.toCharArray();
    for (int i = lastIndex + 1; i < chars.length; i++) {
      if (chars[i] == '/') {
        return jdbcUrl.substring(0, i + 1) + databaseName;
      }
    }
    return jdbcUrl + "/" + databaseName;
  }
}
