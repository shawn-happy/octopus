package com.octopus.spark.operators.runtime.sql;

import com.octopus.spark.operators.declare.common.ColumnDesc;
import java.util.Collections;
import java.util.List;
import java.util.stream.Collectors;
import org.apache.commons.collections4.CollectionUtils;

public abstract class BaseSQLBuilder implements SQLBuilder {

  private final String table;
  private final List<ColumnDesc> columns;

  protected BaseSQLBuilder(String table, List<ColumnDesc> columns) {
    this.table = table;
    this.columns = columns;
  }

  public List<String> getTables() {
    return Collections.singletonList(table);
  }

  public List<ColumnDesc> getColumns() {
    return columns;
  }

  public List<String> getColumnNames() {
    return columns.stream().map(ColumnDesc::getName).collect(Collectors.toList());
  }

  @Override
  public String insert() {
    StringBuilder builder = new StringBuilder();
    sqlClause(builder, "INSERT INTO", getTables(), "", "", "");
    sqlClause(builder, "", getColumnNames(), "(", ")", ", ");
    sqlClause(
        builder,
        "VALUES",
        columns.stream().map(column -> "?").collect(Collectors.toList()),
        "(",
        ")",
        ", ");

    return builder.toString();
  }

  protected void sqlClause(
      StringBuilder builder,
      String keyword,
      List<String> parts,
      String open,
      String close,
      String conjunction) {
    if (builder.length() != 0) {
      builder.append("\n");
    }
    builder.append(keyword);
    builder.append(" ");
    if (CollectionUtils.isNotEmpty(parts)) {
      builder.append(open);
      for (int i = 0, n = parts.size(); i < n; i++) {
        String part = parts.get(i);
        if (i > 0) {
          builder.append(conjunction);
        }
        builder.append(part);
      }
      builder.append(close);
    }
  }
}
