package com.octopus.spark.operators.runtime.sql;

import com.octopus.spark.operators.declare.common.ColumnDesc;
import java.util.List;
import java.util.stream.Collectors;

public class MySQLSQLBuilder extends BaseSQLBuilder {

  public MySQLSQLBuilder(String table, List<ColumnDesc> columns) {
    super(table, columns);
  }

  @Override
  public SQLProvider getProvider() {
    return SQLProvider.MySQL;
  }

  @Override
  public String insertOrUpdate() {
    StringBuilder builder = new StringBuilder();
    sqlClause(builder, "INSERT INTO", getTables(), "", "", "");
    sqlClause(builder, "", getColumnNames(), "(", ")", ", ");
    sqlClause(
        builder,
        "VALUES",
        getColumnNames().stream().map(column -> "?").collect(Collectors.toList()),
        "(",
        ")",
        ", ");
    sqlClause(builder, "ON DUPLICATE KEY UPDATE", null, "", "", "");
    for (ColumnDesc schema :
        getColumns().stream()
            .filter(schemaTerm -> !schemaTerm.isPrimaryKey())
            .collect(Collectors.toList())) {
      builder.append(schema.getName()).append(" = ").append("?").append(", ");
    }
    builder.deleteCharAt(builder.lastIndexOf(", "));
    return builder.toString();
  }
}
