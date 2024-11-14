package io.github.octopus.sql.executor.plugin.api.dialect;

import java.util.List;

public interface MetaDataStatement extends SqlStatement {

  String getDatabaseMetaSql(List<String> databases);

  default String getSchemaMetaSql(String database, List<String> schemas) {
    throw new UnsupportedOperationException();
  }

  String getTableMetaSql(String database, String schema, List<String> tables);

  String getColumnMetaSql(String database, String schema, String table);

  String getIndexMetaSql(String database, String schema, String table);

  String getConstraintMetaSql(String database, String schema, String table);
}
