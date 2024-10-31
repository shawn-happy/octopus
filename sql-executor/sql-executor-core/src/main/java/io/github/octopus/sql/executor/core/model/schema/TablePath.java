package io.github.octopus.sql.executor.core.model.schema;

import java.util.ArrayList;
import java.util.List;
import lombok.Getter;
import lombok.RequiredArgsConstructor;

@Getter
@RequiredArgsConstructor
public class TablePath {
  private final String databaseName;
  private final String schemaName;
  private final String tableName;

  public static final TablePath DEFAULT = TablePath.of("default", "default", "default");

  public static TablePath of(String fullName) {
    return of(fullName, false);
  }

  public static TablePath of(String fullName, boolean schemaFirst) {
    String[] paths = fullName.split("\\.");

    if (paths.length == 1) {
      return of(null, paths[0]);
    }
    if (paths.length == 2) {
      if (schemaFirst) {
        return of(null, paths[0], paths[1]);
      }
      return of(paths[0], null, paths[1]);
    }
    if (paths.length == 3) {
      return of(paths[0], paths[1], paths[2]);
    }
    throw new IllegalArgumentException(
        String.format("Cannot get split '%s' to get databaseName and tableName", fullName));
  }

  public static TablePath of(String databaseName, String tableName) {
    return of(databaseName, null, tableName);
  }

  public static TablePath of(String databaseName, String schemaName, String tableName) {
    return new TablePath(databaseName, schemaName, tableName);
  }

  public String getSchemaAndTableName() {
    return getNameCommon(null, schemaName, tableName, null, null);
  }

  public String getSchemaAndTableName(String quote) {
    return getNameCommon(null, schemaName, tableName, quote, quote);
  }

  public String getFullName() {
    return getNameCommon(databaseName, schemaName, tableName, null, null);
  }

  public String getFullNameWithQuoted() {
    return getFullNameWithQuoted("`");
  }

  public String getFullNameWithQuoted(String quote) {
    return getNameCommon(databaseName, schemaName, tableName, quote, quote);
  }

  public String getFullNameWithQuoted(String quoteLeft, String quoteRight) {
    return getNameCommon(databaseName, schemaName, tableName, quoteLeft, quoteRight);
  }

  private String getNameCommon(
      String databaseName,
      String schemaName,
      String tableName,
      String quoteLeft,
      String quoteRight) {
    List<String> joinList = new ArrayList<>();
    quoteLeft = quoteLeft == null ? "" : quoteLeft;
    quoteRight = quoteRight == null ? "" : quoteRight;

    if (databaseName != null) {
      joinList.add(quoteLeft + databaseName + quoteRight);
    }

    if (schemaName != null) {
      joinList.add(quoteLeft + schemaName + quoteRight);
    }

    if (tableName != null) {
      joinList.add(quoteLeft + tableName + quoteRight);
    }

    return String.join(".", joinList);
  }

  @Override
  public String toString() {
    return getFullName();
  }
}
