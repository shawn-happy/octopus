package com.shawn.octopus.spark.etl.core.common;

import java.util.List;
import org.apache.commons.collections.CollectionUtils;

public class TableDesc {

  private String alias;
  private List<ColumnDesc> columns;

  public String getAlias() {
    return alias;
  }

  public void setAlias(String alias) {
    this.alias = alias;
  }

  public List<ColumnDesc> getColumns() {
    return columns;
  }

  public void setColumns(List<ColumnDesc> columns) {
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
