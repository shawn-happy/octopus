package io.github.octopus.sql.executor.plugin.oracle.dialect;

import io.github.octopus.sql.executor.plugin.api.dialect.JdbcDialect;
import io.github.octopus.sql.executor.plugin.api.dialect.MetaDataStatement;
import java.util.List;

public class OracleMetaDataStatement implements MetaDataStatement {

  private static final OracleMetaDataStatement META_DATA_STATEMENT = new OracleMetaDataStatement();

  private OracleMetaDataStatement() {}

  public static MetaDataStatement getMetaDataStatement() {
    return META_DATA_STATEMENT;
  }

  @Override
  public String getDatabaseMetaSql(List<String> databases) {
    return "";
  }

  @Override
  public String getTableMetaSql(String database, String schema, List<String> tables) {
    return "";
  }

  @Override
  public String getColumnMetaSql(String database, String schema, String table) {
    return "";
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
    return null;
  }
}
