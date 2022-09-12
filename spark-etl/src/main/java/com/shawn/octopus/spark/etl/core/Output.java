package com.shawn.octopus.spark.etl.core;

import java.util.List;
import org.apache.commons.collections.CollectionUtils;

public class Output {

  private String alias;
  private List<ColumnAlias> columns;

  public String getAlias() {
    return alias;
  }

  public void setAlias(String alias) {
    this.alias = alias;
  }

  public List<ColumnAlias> getColumns() {
    return columns;
  }

  public void setColumns(List<ColumnAlias> columns) {
    this.columns = columns;
  }

  public String[] getSelectExprs() {
    if (CollectionUtils.isNotEmpty(columns)) {
      String[] selectExprs = new String[columns.size()];
      for (int i = 0; i < columns.size(); i++) {
        selectExprs[i] = columns.get(i).getSelectExpr();
      }
      return selectExprs;
    }
    return null;
  }
}
