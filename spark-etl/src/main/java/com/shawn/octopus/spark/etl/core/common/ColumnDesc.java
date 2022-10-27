package com.shawn.octopus.spark.etl.core.common;

import com.shawn.octopus.spark.etl.core.enums.FieldType;
import org.apache.commons.lang3.StringUtils;

public class ColumnDesc {

  private final String name;
  private final String alias;
  private final FieldType type;
  private final boolean nullable;
  private final boolean isPrimaryKey;

  public ColumnDesc(
      String name, String alias, FieldType type, boolean nullable, boolean isPrimaryKey) {
    this.name = name;
    this.alias = alias;
    this.type = type;
    this.nullable = nullable;
    this.isPrimaryKey = isPrimaryKey;
  }

  public String getName() {
    return name;
  }

  public String getAlias() {
    return alias;
  }

  public FieldType getType() {
    return type;
  }

  public boolean isNullable() {
    return nullable;
  }

  public boolean isPrimaryKey() {
    return isPrimaryKey;
  }

  public String getSelectExpr() {
    if (StringUtils.isBlank(alias)) {
      return null;
    }
    return String.format("%s as %s", name, alias);
  }

  public static ColumnDescBuilder builder() {
    return new ColumnDescBuilder();
  }

  public static class ColumnDescBuilder {
    private String name;
    private String alias;
    private FieldType type;
    private boolean nullable = false;
    private boolean isPrimaryKey = false;

    public ColumnDescBuilder() {}

    public ColumnDescBuilder name(String name) {
      this.name = name;
      return this;
    }

    public ColumnDescBuilder alias(String alias) {
      this.alias = alias;
      return this;
    }

    public ColumnDescBuilder type(FieldType type) {
      this.type = type;
      return this;
    }

    public ColumnDescBuilder nullable(boolean nullable) {
      this.nullable = nullable;
      return this;
    }

    public ColumnDescBuilder isPrimaryKey(boolean isPrimaryKey) {
      this.isPrimaryKey = isPrimaryKey;
      return this;
    }

    public ColumnDesc build() {
      return new ColumnDesc(this.name, this.alias, this.type, this.nullable, this.isPrimaryKey);
    }
  }
}
