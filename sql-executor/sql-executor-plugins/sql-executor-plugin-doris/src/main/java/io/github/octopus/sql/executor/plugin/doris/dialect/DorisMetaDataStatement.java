package io.github.octopus.sql.executor.plugin.doris.dialect;

import io.github.octopus.sql.executor.core.model.DatabaseIdentifier;
import io.github.octopus.sql.executor.plugin.api.dialect.DialectRegistry;
import io.github.octopus.sql.executor.plugin.api.dialect.JdbcDialect;
import io.github.octopus.sql.executor.plugin.api.dialect.MetaDataStatement;
import java.util.List;

public class DorisMetaDataStatement implements MetaDataStatement {

  private static final DorisMetaDataStatement META_DATA_STATEMENT = new DorisMetaDataStatement();

  private DorisMetaDataStatement() {}

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
    return DialectRegistry.getDialect(DatabaseIdentifier.DORIS);
  }
}
