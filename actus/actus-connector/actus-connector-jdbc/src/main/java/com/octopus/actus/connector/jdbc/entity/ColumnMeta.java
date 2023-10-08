package com.octopus.actus.connector.jdbc.entity;

import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Getter;
import lombok.NoArgsConstructor;

@Getter
@Builder
@NoArgsConstructor
@AllArgsConstructor
public class ColumnMeta {
  private String tableSchema;
  private String tableName;
  private String columnName;
  private String columnDefault;
  private String isNullable;
  private String dataType;
  private long characterMaximumLength;
  private long numericPrecision;
  private long numericScale;
  private long dateTimePrecision;
  private String columnType;
  private String columnKey;
  private String columnComment;
}
