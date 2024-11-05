package io.github.octopus.sql.executor.core.model.schema;

import io.github.octopus.sql.executor.core.model.FieldIdeEnum;
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

  public String getDatabaseNameWithQuoted(FieldIdeEnum fieldIdeEnum) {
    return getDatabaseNameWithQuoted("`", fieldIdeEnum);
  }

  public String getDatabaseNameWithQuoted(String quote, FieldIdeEnum fieldIdeEnum) {
    return getDatabaseNameWithQuoted(quote, quote, fieldIdeEnum);
  }

  public String getDatabaseNameWithQuoted(
      String quoteLeft, String quoteRight, FieldIdeEnum fieldIdeEnum) {
    return getNameCommon(databaseName, null, null, quoteLeft, quoteRight, fieldIdeEnum);
  }

  public String getSchemaNameWithQuoted(FieldIdeEnum fieldIdeEnum) {
    return getSchemaNameWithQuoted("`", fieldIdeEnum);
  }

  public String getSchemaNameWithQuoted(String quote, FieldIdeEnum fieldIdeEnum) {
    return getSchemaNameWithQuoted(quote, quote, fieldIdeEnum);
  }

  public String getSchemaNameWithQuoted(
      String quoteLeft, String quoteRight, FieldIdeEnum fieldIdeEnum) {
    return getNameCommon(databaseName, schemaName, null, quoteLeft, quoteRight, fieldIdeEnum);
  }

  public String getFullName() {
    return getNameCommon(databaseName, schemaName, tableName, null, null, FieldIdeEnum.ORIGINAL);
  }

  public String getFullNameWithQuoted(FieldIdeEnum fieldIdeEnum) {
    return getFullNameWithQuoted("`", fieldIdeEnum);
  }

  public String getFullNameWithQuoted(String quote, FieldIdeEnum fieldIdeEnum) {
    return getNameCommon(databaseName, schemaName, tableName, quote, quote, fieldIdeEnum);
  }

  public String getFullNameWithQuoted(
      String quoteLeft, String quoteRight, FieldIdeEnum fieldIdeEnum) {
    return getNameCommon(databaseName, schemaName, tableName, quoteLeft, quoteRight, fieldIdeEnum);
  }

  private String getNameCommon(
      String databaseName,
      String schemaName,
      String tableName,
      String quoteLeft,
      String quoteRight,
      FieldIdeEnum fieldIde) {
    List<String> joinList = new ArrayList<>();
    quoteLeft = quoteLeft == null ? "" : quoteLeft;
    quoteRight = quoteRight == null ? "" : quoteRight;

    if (databaseName != null) {
      joinList.add(quoteLeft + identifier(databaseName, fieldIde) + quoteRight);
    }

    if (schemaName != null) {
      joinList.add(quoteLeft + identifier(schemaName, fieldIde) + quoteRight);
    }

    if (tableName != null) {
      joinList.add(quoteLeft + identifier(tableName, fieldIde) + quoteRight);
    }
    return String.join(".", joinList);
  }

  private static String identifier(String token, FieldIdeEnum fieldIde) {
    if (fieldIde == null) {
      return token;
    }
    switch (fieldIde) {
      case LOWERCASE:
        return token.toLowerCase();
      case UPPERCASE:
        return token.toUpperCase();
      default:
        return token;
    }
  }

  @Override
  public String toString() {
    return getFullName();
  }
}
