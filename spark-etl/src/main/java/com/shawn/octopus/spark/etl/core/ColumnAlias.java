package com.shawn.octopus.spark.etl.core;

import org.apache.commons.lang3.StringUtils;

public class ColumnAlias {

  private String column;
  private String alias;

  private ColumnAlias(String column, String alias) {
    this.column = column;
    this.alias = alias;
  }

  public String getColumn() {
    return column;
  }

  public String getAlias() {
    if (StringUtils.isEmpty(alias)) {
      this.alias = column;
    }
    return alias;
  }

  public String getSelectExpr() {
    return String.format("%s as %s", getColumn(), getAlias());
  }

  public static ColumnAliasBuilder builder() {
    return new ColumnAliasBuilder();
  }

  public static class ColumnAliasBuilder {
    private String column;
    private String alias;

    public ColumnAliasBuilder column(String column) {
      this.column = column;
      return this;
    }

    public ColumnAliasBuilder alias(String alias) {
      this.alias = alias;
      return this;
    }

    public ColumnAlias build() {
      return new ColumnAlias(this.column, this.alias);
    }
  }
}
