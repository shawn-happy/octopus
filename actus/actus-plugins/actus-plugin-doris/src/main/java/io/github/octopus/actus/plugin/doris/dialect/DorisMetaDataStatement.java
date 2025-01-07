package io.github.octopus.actus.plugin.doris.dialect;

import io.github.octopus.actus.core.model.DatabaseIdentifier;
import io.github.octopus.actus.plugin.api.dialect.DialectRegistry;
import io.github.octopus.actus.plugin.api.dialect.JdbcDialect;
import io.github.octopus.actus.plugin.api.dialect.MetaDataStatement;
import java.util.ArrayList;
import java.util.List;
import java.util.stream.Collectors;
import org.apache.commons.collections4.CollectionUtils;
import org.apache.commons.lang3.StringUtils;

public class DorisMetaDataStatement implements MetaDataStatement {

  private static final DorisMetaDataStatement META_DATA_STATEMENT = new DorisMetaDataStatement();

  private DorisMetaDataStatement() {}

  public static MetaDataStatement getMetaDataStatement() {
    return META_DATA_STATEMENT;
  }

  private static final List<String> SYSTEM_DATABASES =
      List.of("mysql", "information_schema", "performance_schema", "sys");

  private static final String DATABASE_METADATA_QUERY_SQL =
      "SELECT `schema_name` AS `name`, \n"
          + "`default_character_set_name` AS `characterSet`, \n"
          + "`default_collation_name` AS `collation` \n"
          + "FROM `information_schema`.`SCHEMATA` \n";

  private static final String TABLE_METADATA_QUERY_SQL =
      "SELECT `TABLE_SCHEMA` AS `database`, \n"
          + "`TABLE_NAME` AS `table`, \n"
          + "`ENGINE`, \n"
          + "`TABLE_ROWS` AS `rowNumber`, \n"
          + "`DATA_LENGTH` AS `rowSize`, \n"
          + "`CREATE_TIME` AS `createTime`, \n"
          + "`UPDATE_TIME` AS `updateTime`, \n"
          + "`TABLE_COMMENT` AS `comment` \n"
          + "FROM `information_schema`.`TABLES`";

  private static final String COLUMN_METADATA_QUERY_SQL =
      "SELECT `TABLE_SCHEMA` AS `database`,\n"
          + "`TABLE_NAME` AS `table`,\n"
          + "`COLUMN_NAME` AS `column`,\n"
          + "`COLUMN_DEFAULT` AS `default_value`,\n"
          + "`IS_NULLABLE` AS `nullable`,\n"
          + "`DATA_TYPE`,\n"
          + "`CHARACTER_MAXIMUM_LENGTH` AS `length`,\n"
          + "`NUMERIC_PRECISION` AS `percision`,\n"
          + "`NUMERIC_SCALE` AS `scale`,\n"
          + "`DATETIME_PRECISION` AS `time_precision`,\n"
          + "`COLUMN_TYPE`,\n"
          + "`COLUMN_COMMENT` AS `comment`\n"
          + "FROM `information_schema`.`COLUMNS`";

  @Override
  public String getDatabaseMetaSql(List<String> databases) {
    if (CollectionUtils.isNotEmpty(databases)) {
      if (databases.size() == 1) {
        return String.format("%s WHERE `schema_name` = ?", DATABASE_METADATA_QUERY_SQL);
      }
      return String.format(
          "%s WHERE `schema_name` IN (%s)",
          DATABASE_METADATA_QUERY_SQL,
          databases.stream().map(db -> "?").collect(Collectors.joining(", ")));
    }

    return String.format(
        "%s WHERE `schema_name` NOT IN (%s)",
        DATABASE_METADATA_QUERY_SQL,
        SYSTEM_DATABASES
            .stream()
            .map(db -> String.format("'%s'", db))
            .collect(Collectors.joining(", ")));
  }

  @Override
  public String getTableMetaSql(String database, String schema, List<String> tables) {
    List<String> whereSql = new ArrayList<>();
    if (StringUtils.isNotBlank(database)) {
      whereSql.add("`TABLE_SCHEMA` = ?");
    }
    if (CollectionUtils.isNotEmpty(tables)) {
      if (tables.size() == 1) {
        whereSql.add("`TABLE_NAME` = ?");
      } else {
        whereSql.add(
            String.format(
                "`TABLE_NAME` IN (%s)",
                tables.stream().map(t -> "?").collect(Collectors.joining(", "))));
      }
    }
    if (whereSql.size() == 1) {
      return String.format("%s WHERE %s", TABLE_METADATA_QUERY_SQL, whereSql.get(0));
    } else if (whereSql.size() > 1) {
      return String.format("%s WHERE %s", TABLE_METADATA_QUERY_SQL, String.join(" AND ", whereSql));
    }
    return String.format(
        "%s WHERE `TABLE_SCHEMA` NOT IN (%s) AND `engine` IS NOT NULL",
        TABLE_METADATA_QUERY_SQL,
        SYSTEM_DATABASES
            .stream()
            .map(db -> String.format("'%s'", db))
            .collect(Collectors.joining(", ")));
  }

  @Override
  public String getColumnMetaSql(String database, String schema, String table) {
    List<String> whereSql = new ArrayList<>();
    if (StringUtils.isNotBlank(database)) {
      whereSql.add("`TABLE_SCHEMA` = ?");
    }
    if (StringUtils.isNotBlank(table)) {
      whereSql.add("`TABLE_NAME` = ?");
    }
    if (whereSql.size() == 1) {
      return String.format("%s WHERE %s", COLUMN_METADATA_QUERY_SQL, whereSql.get(0));
    } else if (whereSql.size() > 1) {
      return String.format(
          "%s WHERE %s", COLUMN_METADATA_QUERY_SQL, String.join(" AND ", whereSql));
    }
    return String.format(
        "%s WHERE `TABLE_SCHEMA` NOT IN (%s)",
        COLUMN_METADATA_QUERY_SQL,
        SYSTEM_DATABASES
            .stream()
            .map(db -> String.format("'%s'", db))
            .collect(Collectors.joining(", ")));
  }

  @Override
  public String getIndexMetaSql(String database, String schema, String table) {
    return "";
  }

  @Override
  public String getConstraintMetaSql(String database, String schema, String table) {
    return "";
  }

  @Override
  public JdbcDialect getJdbcDialect() {
    return DialectRegistry.getDialect(DatabaseIdentifier.DORIS);
  }
}
