package io.github.octopus.actus.plugin.sqlserver.dialect;

import io.github.octopus.actus.core.model.DatabaseIdentifier;
import io.github.octopus.actus.plugin.api.dialect.DialectRegistry;
import io.github.octopus.actus.plugin.api.dialect.JdbcDialect;
import io.github.octopus.actus.plugin.api.dialect.MetaDataStatement;
import java.util.ArrayList;
import java.util.List;
import java.util.stream.Collectors;
import org.apache.commons.collections4.CollectionUtils;
import org.apache.commons.lang3.StringUtils;

public class SqlServerMetaDataStatement implements MetaDataStatement {

  private static final SqlServerMetaDataStatement META_DATA_STATEMENT =
      new SqlServerMetaDataStatement();

  private SqlServerMetaDataStatement() {}

  public static MetaDataStatement getMetaDataStatement() {
    return META_DATA_STATEMENT;
  }

  private static final List<String> SYS_DATABASE = List.of("master", "model", "msdb", "tempdb");

  private static final String DATABASE_METADATA_QUERY_SQL =
      "SELECT name, collation_name FROM sys.databases";

  private static final String SCHEMA_METADATA_QUERY_SQL =
      "SELECT name, schema_id as id FROM %s.sys.schemas";

  private static final String TABLE_METADATA_QUERY_SQL =
      "select '%s' as [database],\n"
          + "        t.name as [table],\n"
          + "        s.name as [schema],\n"
          + "        ddps.row_count as [row_number],\n"
          + "        ddps.reserved_page_count * 8 as [row_size],\n"
          + "        t.create_date as [create_time],\n"
          + "        t.modify_date as [update_time],\n"
          + "        ep.value as [comment]\n"
          + "        from %s.sys.tables t\n"
          + "        left join %s.sys.schemas s\n"
          + "        on t.schema_id = s.schema_id\n"
          + "        left join (select object_id, sum(row_count) as [row_count], sum(reserved_page_count) as [reserved_page_count]\n"
          + "        from %s.sys.dm_db_partition_stats\n"
          + "        group by object_id) ddps on t.object_id = ddps.object_id\n"
          + "        left join %s.sys.extended_properties ep\n"
          + "        on t.object_id = ep.major_id and ep.minor_id = 0 and ep.name = 'MS_Description'";

  private static final String COLUMN_METADATA_QUERY_SQL =
      "SELECT\n"
          + "        '%s' AS [database],\n"
          + "        s.name AS [schema],\n"
          + "        tbl.name AS [table],\n"
          + "        col.name AS [column],\n"
          + "        ext.value AS [comment],\n"
          + "        types.name AS data_type,\n"
          + "        col.max_length AS length,\n"
          + "        col.precision AS precision,\n"
          + "        col.scale AS scale,\n"
          + "        col.is_nullable AS nullable,\n"
          + "        def.definition AS default_value\n"
          + "        FROM %s.sys.tables tbl\n"
          + "        inner join %s.sys.schemas s on tbl.schema_id = s.schema_id\n"
          + "        INNER JOIN %s.sys.columns col ON tbl.object_id = col.object_id\n"
          + "        LEFT JOIN %s.sys.types types ON col.user_type_id = types.user_type_id\n"
          + "        LEFT JOIN %s.sys.extended_properties ext ON ext.major_id = col.object_id AND ext.minor_id =\n"
          + "        col.column_id\n"
          + "        LEFT JOIN %s.sys.default_constraints def\n"
          + "        ON col.default_object_id = def.object_id AND ext.minor_id = col.column_id AND\n"
          + "        ext.name = 'MS_Description'";

  private static final String CONSTRAINT_METADATA_SQL =
      "SELECT istc.TABLE_CATALOG as [database],\n"
          + "        istc.TABLE_SCHEMA as [schema],\n"
          + "        istc.TABLE_NAME as [table],\n"
          + "        istc.CONSTRAINT_NAME as [name],\n"
          + "        istc.CONSTRAINT_TYPE as [type],\n"
          + "        iskcu.COLUMN_NAME as [column]\n"
          + "        FROM %s.INFORMATION_SCHEMA.TABLE_CONSTRAINTS istc\n"
          + "        inner join %s.INFORMATION_SCHEMA.KEY_COLUMN_USAGE iskcu\n"
          + "        on istc.CONSTRAINT_CATALOG = iskcu.CONSTRAINT_CATALOG and\n"
          + "        istc.CONSTRAINT_SCHEMA = iskcu.CONSTRAINT_SCHEMA and istc.CONSTRAINT_NAME = iskcu.CONSTRAINT_NAME";

  @Override
  public String getDatabaseMetaSql(List<String> databases) {
    if (CollectionUtils.isNotEmpty(databases)) {
      return String.format(
          "%s WHERE name IN (%s)",
          DATABASE_METADATA_QUERY_SQL,
          databases.stream().map(db -> "?").collect(Collectors.joining(", ")));
    }
    return String.format(
        "%s WHERE name NOT IN (%s)",
        DATABASE_METADATA_QUERY_SQL,
        SYS_DATABASE
            .stream()
            .map(db -> String.format("'%s'", db))
            .collect(Collectors.joining(", ")));
  }

  @Override
  public String getSchemaMetaSql(String database, List<String> schemas) {
    if (CollectionUtils.isNotEmpty(schemas)) {
      return String.format(
          "%s WHERE name IN (%s)",
          String.format(SCHEMA_METADATA_QUERY_SQL, database),
          schemas.stream().map(db -> "?").collect(Collectors.joining(", ")));
    }
    return String.format(
        "%s WHERE name not in ('sys', 'dbo', 'guest', 'INFORMATION_SCHEMA') AND name not like '%s'",
        String.format(SCHEMA_METADATA_QUERY_SQL, database), "%db_%");
  }

  @Override
  public String getTableMetaSql(String database, String schema, List<String> tables) {
    String sql =
        String.format(TABLE_METADATA_QUERY_SQL, database, database, database, database, database);
    List<String> whereSql = new ArrayList<>();
    if (StringUtils.isNotBlank(schema)) {
      whereSql.add("s.name = ?");
    } else {
      whereSql.add(
          "s.name not in ('sys', 'dbo', 'guest', 'INFORMATION_SCHEMA') and s.name not like '%db_%'");
    }
    if (CollectionUtils.isNotEmpty(tables)) {
      whereSql.add(
          String.format(
              "t.name in (%s)", tables.stream().map(db -> "?").collect(Collectors.joining(", "))));
    }
    if (CollectionUtils.isNotEmpty(whereSql)) {
      String where = String.join(" AND ", whereSql);
      return String.format("%s WHERE %s", sql, where);
    }
    return sql;
  }

  @Override
  public String getColumnMetaSql(String database, String schema, String table) {
    String sql =
        String.format(
            COLUMN_METADATA_QUERY_SQL,
            database,
            database,
            database,
            database,
            database,
            database,
            database);

    List<String> whereSql = new ArrayList<>();
    if (StringUtils.isNotBlank(schema)) {
      whereSql.add("s.name = ?");
    } else {
      whereSql.add(
          "s.name not in ('sys', 'dbo', 'guest', 'INFORMATION_SCHEMA') and s.name not like '%db_%'");
    }
    if (StringUtils.isNotBlank(table)) {
      whereSql.add("tbl.name = ?");
    }
    if (CollectionUtils.isNotEmpty(whereSql)) {
      String where = String.join(" AND ", whereSql);
      return String.format("%s WHERE %s", sql, where);
    }
    return sql;
  }

  @Override
  public String getIndexMetaSql(String database, String schema, String table) {
    return "";
  }

  @Override
  public String getConstraintMetaSql(String database, String schema, String table) {
    String sql = String.format(CONSTRAINT_METADATA_SQL, database, database);
    List<String> whereSql = new ArrayList<>();
    if (StringUtils.isNotBlank(schema)) {
      whereSql.add("istc.TABLE_SCHEMA = ?");
    } else {
      whereSql.add(
          "istc.TABLE_SCHEMA ('sys', 'dbo', 'guest', 'INFORMATION_SCHEMA') and s.name not like '%db_%'");
    }
    if (StringUtils.isNotBlank(table)) {
      whereSql.add("istc.TABLE_NAME = ?");
    }
    if (CollectionUtils.isNotEmpty(whereSql)) {
      String where = String.join(" AND ", whereSql);
      return String.format("%s WHERE %s", sql, where);
    }
    return sql;
  }

  @Override
  public JdbcDialect getJdbcDialect() {
    return DialectRegistry.getDialect(DatabaseIdentifier.SQLSERVER);
  }
}
