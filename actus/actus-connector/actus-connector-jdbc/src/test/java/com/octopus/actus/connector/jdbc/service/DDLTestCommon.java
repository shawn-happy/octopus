package com.octopus.actus.connector.jdbc.service;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.ResultSetMetaData;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.TimeUnit;
import org.apache.commons.collections4.CollectionUtils;
import org.apache.commons.lang3.StringUtils;

public class DDLTestCommon {

  private final String url;
  private final String username;
  private final String password;
  private final String driverClass;

  public DDLTestCommon(String url, String username, String password, String driverClass) {
    this.url = url;
    this.username = username;
    this.password = password;
    this.driverClass = driverClass;
  }

  public boolean hasDatabase(String database) {
    final List<Map<String, String>> databases = runShowTemplate("show databases");
    for (Map<String, String> rs : databases) {
      final String value = rs.get("Database");
      if (database.equalsIgnoreCase(value)) {
        return true;
      }
    }
    return false;
  }

  public boolean hasTable(String database, String table) {
    final List<Map<String, String>> tables = runShowTemplate("show tables from `" + database + "`");
    for (Map<String, String> rs : tables) {
      final String value = rs.get(String.format("Tables_in_%s", database));
      if (table.equalsIgnoreCase(value)) {
        return true;
      }
    }
    return false;
  }

  public boolean hasColumn(String database, String table, String column) {
    final List<Map<String, String>> tables =
        runShowTemplate(String.format("show columns from `%s`.`%s`", database, table));
    for (Map<String, String> rs : tables) {
      final String value = rs.get("Field");
      if (column.equalsIgnoreCase(value)) {
        return true;
      }
    }
    return false;
  }

  public boolean matchColumnType(String database, String table, String column, String type) {
    final List<Map<String, String>> tables =
        runShowTemplate(String.format("show columns from `%s`.`%s`", database, table));
    for (Map<String, String> rs : tables) {
      final String value = rs.get("Field");
      final String columnType = rs.get("Type");
      if (column.equalsIgnoreCase(value) && type.equalsIgnoreCase(columnType)) {
        return true;
      }
    }
    return false;
  }

  public boolean columnIsPk(String database, String table, String column) {
    final List<Map<String, String>> tables =
        runShowTemplate(String.format("show columns from `%s`.`%s`", database, table));
    for (Map<String, String> rs : tables) {
      final String value = rs.get("Field");
      final String key = rs.get("Key");
      if (column.equalsIgnoreCase(value) && "PRI".equalsIgnoreCase(key)) {
        return true;
      }
    }
    return false;
  }

  public boolean columnIsNullable(String database, String table, String column) {
    final List<Map<String, String>> tables =
        runShowTemplate(String.format("show columns from `%s`.`%s`", database, table));
    for (Map<String, String> rs : tables) {
      final String value = rs.get("Field");
      final String nullable = rs.get("Null");
      if (column.equalsIgnoreCase(value) && "YES".equalsIgnoreCase(nullable)) {
        return true;
      }
    }
    return false;
  }

  public boolean columnHasDefaultValue(String database, String table, String column) {
    final List<Map<String, String>> tables =
        runShowTemplate(String.format("show columns from `%s`.`%s`", database, table));
    for (Map<String, String> rs : tables) {
      final String value = rs.get("Field");
      final String defaultValue = rs.get("Default");
      if (column.equalsIgnoreCase(value) && StringUtils.isNotBlank(defaultValue)) {
        return true;
      }
    }
    return false;
  }

  public boolean hasIndex(String database, String table, String index) {
    final List<Map<String, String>> indexes =
        runShowTemplate(String.format("show indexes from `%s`.`%s`", database, table));
    for (Map<String, String> rs : indexes) {
      final String value = rs.get("Key_name");
      if (index.equalsIgnoreCase(value)) {
        return true;
      }
    }
    return false;
  }

  public boolean matchIndexType(String database, String table, String index, String indexType) {
    final List<Map<String, String>> indexes =
        runShowTemplate(String.format("show indexes from `%s`.`%s`", database, table));
    for (Map<String, String> rs : indexes) {
      final String value = rs.get("Key_name");
      final String type = rs.get("Index_type");
      if (index.equalsIgnoreCase(value) && indexType.equalsIgnoreCase(type)) {
        return true;
      }
    }
    return false;
  }

  public boolean matchTableComment(String database, String table, String comment) {
    String sql =
        "SELECT table_comment FROM information_schema.TABLES WHERE table_schema = '"
            + database
            + "' and table_name = '"
            + table
            + "'";
    final List<Map<String, String>> results = runShowTemplate(sql);
    if (CollectionUtils.isNotEmpty(results)) {
      for (Map<String, String> result : results) {
        if (StringUtils.isBlank(comment) && StringUtils.isBlank(result.get("TABLE_COMMENT"))) {
          return true;
        }
        if (result.get("TABLE_COMMENT").equals(comment)) {
          return true;
        }
      }
    }
    return false;
  }

  public boolean matchColumnComment(String database, String table, String column, String comment) {
    String sql =
        "select COLUMN_NAME,column_comment,column_type,column_key from information_schema.columns where table_schema = '"
            + database
            + "' and table_name = '"
            + table
            + "'"
            + " and column_name = '"
            + column
            + "'";
    final List<Map<String, String>> results = runShowTemplate(sql);
    if (CollectionUtils.isNotEmpty(results)) {
      for (Map<String, String> result : results) {
        final String columnComment = result.get("COLUMN_COMMENT");
        if (StringUtils.isBlank(columnComment) && StringUtils.isBlank(comment)) {
          return true;
        }
        if (StringUtils.equals(comment, columnComment)) {
          return true;
        }
      }
    }
    return false;
  }

  public boolean alterTableDropColumnSuccess(String database, String table) throws Exception {
    int retry = 0;
    String sql = "SHOW ALTER TABLE COLUMN from `" + database + "`";
    while (retry < 3) {
      final List<Map<String, String>> results = runShowTemplate(sql);
      for (Map<String, String> result : results) {
        final String tableName = result.get("TableName");
        if (table.equalsIgnoreCase(tableName) && "FINISHED".equalsIgnoreCase(result.get("State"))) {
          return true;
        }
      }
      retry++;
      TimeUnit.SECONDS.sleep(1);
    }
    return false;
  }

  private List<Map<String, String>> runShowTemplate(String sql) {
    try {
      Class.forName(driverClass).newInstance();
    } catch (Exception e) {
      throw new RuntimeException(e);
    }

    try (Connection conn = DriverManager.getConnection(url, username, password);
        Statement statement = conn.createStatement();
        final ResultSet resultSet = statement.executeQuery(sql)) {

      final ResultSetMetaData metaData = resultSet.getMetaData();
      List<Map<String, String>> results = new ArrayList<>(resultSet.getFetchSize());
      while (resultSet.next()) {
        Map<String, String> result = new HashMap<>();
        for (int i = 1; i <= metaData.getColumnCount(); i++) {
          final String value = resultSet.getString(i);
          final String name = metaData.getColumnName(i);
          result.put(name, value);
        }
        results.add(result);
      }
      return results;
    } catch (Exception e) {
      throw new RuntimeException(e);
    }
  }
}
