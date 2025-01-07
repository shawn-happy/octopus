package io.github.octopus.actus.plugin.oracle.dialect;

import io.github.octopus.actus.core.model.DatabaseIdentifier;
import io.github.octopus.actus.plugin.api.dialect.DialectRegistry;
import io.github.octopus.actus.plugin.api.dialect.JdbcDialect;
import io.github.octopus.actus.plugin.api.dialect.MetaDataStatement;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.stream.Collectors;
import org.apache.commons.collections4.CollectionUtils;
import org.apache.commons.lang3.StringUtils;

public class OracleMetaDataStatement implements MetaDataStatement {

  private static final OracleMetaDataStatement META_DATA_STATEMENT = new OracleMetaDataStatement();

  private OracleMetaDataStatement() {}

  public static MetaDataStatement getMetaDataStatement() {
    return META_DATA_STATEMENT;
  }

  private static final List<String> SYS_SCHEMAS =
      List.of(
          "ANONYMOUS",
          "APPQOSSYS",
          "APEX_030200",
          "APEX_PUBLIC_USER",
          "AUDSYS",
          "CTXSYS",
          "DVSYS",
          "DBSFWUSER",
          "DBSNMP",
          "GSMADMIN_INTERNAL",
          "EXFSYS",
          "FLOWS_FILES",
          "LBACSYS",
          "MDDATA",
          "MDSYS",
          "MGMT_VIEW",
          "OJVMSYS",
          "OLAPSYS",
          "ORDDATA",
          "ORDSYS",
          "ORDPLUGINS",
          "OWBSYS",
          "OWBSYS_AUDIT",
          "OUTLN",
          "SYS",
          "SYSTEM",
          "SYSMAN",
          "WMSYS",
          "XDB");

  private static final String DATABASE_METADATA_QUERY_SQL = "SELECT NAME FROM V$DATABASE";

  private static final String TABLE_METADATA_QUERY_SQL =
      "select ao.OWNER     as \"database\",\n"
          + "           ut.TABLE_NAME    as \"table\",\n"
          + "           ut.NUM_ROWS      as \"row_number\",\n"
          + "           ut.BLOCKS * 8192 as \"row_size\",\n"
          + "           ao.CREATED       as \"create_time\",\n"
          + "           ao.LAST_DDL_TIME as \"update_time\",\n"
          + "           utc.COMMENTS     as \"comment\"\n"
          + "        from USER_TABLES ut\n"
          + "            left join ALL_OBJECTS ao\n"
          + "        on ut.TABLE_NAME = ao.OBJECT_NAME\n"
          + "            left join USER_TAB_COMMENTS utc\n"
          + "            on ut.TABLE_NAME = utc.TABLE_NAME\n"
          + "        where ao.OBJECT_TYPE = 'TABLE'";

  private static final String COLUMN_METADATA_QUERY_SQL =
      "        SELECT cols.OWNER                                           AS \"DATABASE\",\n"
          + "               cols.TABLE_NAME                                      AS \"TABLE\",\n"
          + "               cols.COLUMN_NAME                                     AS \"COLUMN\",\n"
          + "                CASE\n"
          + "                    WHEN cols.data_type LIKE 'INTERVAL%%' THEN 'INTERVAL'\n"
          + "                    ELSE REGEXP_SUBSTR(cols.data_type, '^[^(]+')\n"
          + "                END                                              AS DATA_TYPE,\n"
          + "                cols.data_type ||\n"
          + "                CASE\n"
          + "                    WHEN cols.data_type IN ('VARCHAR', 'VARCHAR2', 'CHAR') THEN '(' || cols.data_length || ')'\n"
          + "                    WHEN cols.data_type IN ('NVARCHAR2', 'NCHAR') THEN '(' || cols.char_length || ')'\n"
          + "                    WHEN cols.data_type IN ('NUMBER') AND cols.data_precision IS NOT NULL AND cols.data_scale IS NOT NULL\n"
          + "                THEN '(' || cols.data_precision || ', ' || cols.data_scale || ')'\n"
          + "                    WHEN cols.data_type IN ('NUMBER') AND cols.data_precision IS NOT NULL AND cols.data_scale IS NULL\n"
          + "                THEN '(' || cols.data_precision || ')'\n"
          + "                    WHEN cols.data_type IN ('RAW') THEN '(' || cols.data_length || ')'\n"
          + "                END                                              AS COLUMN_TYPE,\n"
          + "                cols.data_length                                     AS LENGTH,\n"
          + "                cols.data_precision                                  AS PRECISION,\n"
          + "                cols.data_scale                                      AS SCALE,\n"
          + "                com.comments                                         AS\n"
          + "                \"COMMENT\",\n"
          + "                cols.data_default                                    AS DEFAULT_VALUE,\n"
          + "                CASE cols.nullable WHEN 'N' THEN 'NO' ELSE 'YES' END AS NULLABLE\n"
          + "        FROM all_tab_columns cols\n"
          + "            LEFT JOIN all_col_comments com\n"
          + "            ON cols.table_name = com.table_name AND cols.column_name = com.column_name AND cols.owner = com.owner\n";

  private static final String CONSTRAINT_METADATA_QUERY_SQL =
      "        select uc.CONSTRAINT_NAME as name,\n"
          + "               uc.CONSTRAINT_TYPE as type,\n"
          + "               ucc.COLUMN_NAME    as \"column\",\n"
          + "               ucc.OWNER          as \"database\",\n"
          + "               ucc.TABLE_NAME     as \"table\"\n"
          + "        from USER_CONSTRAINTS uc\n"
          + "                 left join USER_CONS_COLUMNS ucc\n"
          + "                           on uc.OWNER = ucc.OWNER\n"
          + "                               and uc.TABLE_NAME = ucc.TABLE_NAME\n"
          + "                               and uc.CONSTRAINT_NAME = ucc.CONSTRAINT_NAME";

  @Override
  public String getDatabaseMetaSql(List<String> databases) {
    if (CollectionUtils.isNotEmpty(databases)) {
      String[] values = new String[databases.size()];
      Arrays.fill(values, "?");
      return String.format(
          "%s WHERE NAME in (%s)", DATABASE_METADATA_QUERY_SQL, String.join(", ", values));
    }
    return String.format(
        "%s WHERE NAME NOT IN (%s)", DATABASE_METADATA_QUERY_SQL, String.join(", ", SYS_SCHEMAS));
  }

  @Override
  public String getTableMetaSql(String database, String schema, List<String> tables) {
    List<String> whereSql = new ArrayList<>();
    if (StringUtils.isNotBlank(database)) {
      whereSql.add("ao.OWNER = ?");
    }
    if (StringUtils.isBlank(database) && CollectionUtils.isNotEmpty(tables)) {
      whereSql.add(
          String.format(
              "ao.OWNER NOT IN (%s) AND ut.TABLE_NAME IN (%s)",
              String.join(", ", SYS_SCHEMAS),
              tables.stream().map(t -> "?").collect(Collectors.joining(", "))));
    } else if (CollectionUtils.isNotEmpty(tables)) {
      whereSql.add(
          String.format(
              "ut.TABLE_NAME IN (%s)",
              tables.stream().map(t -> "?").collect(Collectors.joining(", "))));
    }
    String andSql = null;
    if (CollectionUtils.isNotEmpty(whereSql)) {
      andSql = String.join(" AND ", whereSql);
    }
    if (StringUtils.isNotBlank(andSql)) {
      return String.format("%s AND %s", TABLE_METADATA_QUERY_SQL, andSql);
    }
    return TABLE_METADATA_QUERY_SQL;
  }

  @Override
  public String getColumnMetaSql(String database, String schema, String table) {
    List<String> whereSql = new ArrayList<>();
    if (StringUtils.isNotBlank(database)) {
      whereSql.add("cols.OWNER = ?");
    }
    if (StringUtils.isNotBlank(table)) {
      whereSql.add("cols.TABLE_NAME = ?");
    }
    String where = CollectionUtils.isEmpty(whereSql) ? null : String.join(" AND ", whereSql);
    if (StringUtils.isBlank(where)) {
      return String.format(
          "%s WHERE cols.OWNER NOT IN (%s)",
          COLUMN_METADATA_QUERY_SQL,
          SYS_SCHEMAS.stream().map(this::quoteIdentifier).collect(Collectors.joining(", ")));
    }
    return String.format("%s WHERE %s", COLUMN_METADATA_QUERY_SQL, where);
  }

  @Override
  public String getIndexMetaSql(String database, String schema, String table) {
    return "";
  }

  @Override
  public String getConstraintMetaSql(String database, String schema, String table) {
    List<String> whereSql = new ArrayList<>();
    if (StringUtils.isNotBlank(database)) {
      whereSql.add("uc.OWNER = ?");
    }
    if (StringUtils.isNotBlank(table)) {
      whereSql.add("uc.TABLE_NAME = ?");
    }
    String where = CollectionUtils.isEmpty(whereSql) ? null : String.join(" AND ", whereSql);
    if (StringUtils.isBlank(where)) {
      return String.format(
          "%s WHERE uc.OWNER NOT IN (%s)",
          CONSTRAINT_METADATA_QUERY_SQL,
          SYS_SCHEMAS.stream().map(this::quoteIdentifier).collect(Collectors.joining(", ")));
    }
    return String.format("%s WHERE %s", CONSTRAINT_METADATA_QUERY_SQL, where);
  }

  @Override
  public JdbcDialect getJdbcDialect() {
    return DialectRegistry.getDialect(DatabaseIdentifier.ORACLE);
  }
}
