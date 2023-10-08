package com.octopus.actus.connector.jdbc.model;

import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Getter;
import lombok.NoArgsConstructor;

@Getter
@Builder
@NoArgsConstructor
@AllArgsConstructor
public class ColumnMetaInfo {
  private String databaseName;
  private String tableName;
  private String columnName;
  private String defaultValue;
  private boolean nullable;
  private FieldType fieldType;
  private long precision;
  private long scale;
  private ColumnKey columnKey;
  private String comment;
}
