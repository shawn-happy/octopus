package com.octopus.operators.engine.table.catalog;

import com.octopus.operators.engine.table.type.RowDataType;
import com.octopus.operators.engine.table.type.RowDataTypeParse;
import org.apache.commons.lang3.StringUtils;
import org.jetbrains.annotations.NotNull;

public class Column {
  @NotNull private String name;
  @NotNull private String fieldType;
  private String alias;
  private boolean nullable;
  private boolean primaryKey;
  private Object defaultValue;
  private transient RowDataType rowDataType;

  public Column() {}

  public Column(String name, String fieldType) {
    this(name, fieldType, null, true, false, null);
  }

  public Column(
      @NotNull String name,
      @NotNull String fieldType,
      String alias,
      boolean nullable,
      boolean primaryKey,
      Object defaultValue) {
    this.name = name;
    this.alias = StringUtils.isNotBlank(alias) ? alias : name;
    this.fieldType = fieldType;
    this.rowDataType = RowDataTypeParse.parseDataType(fieldType);
    this.nullable = nullable;
    this.primaryKey = primaryKey;
    this.defaultValue = defaultValue;
  }

  public RowDataType getRowDataType() {
    return rowDataType;
  }

  public String getAlias() {
    return alias;
  }

  public @NotNull String getName() {
    return name;
  }

  public @NotNull String getFieldType() {
    return fieldType;
  }

  public boolean isNullable() {
    return nullable;
  }

  public boolean isPrimaryKey() {
    return primaryKey;
  }

  public Object getDefaultValue() {
    return defaultValue;
  }
}
