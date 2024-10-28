package io.github.octopus.sql.executor.core.entity;

import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Getter;
import lombok.NoArgsConstructor;

@Getter
@Builder
@NoArgsConstructor
@AllArgsConstructor
public class Column {
  private String columnName;
  private String fieldType;
  private String comment;
  private boolean nullable;
  // 在oracle jdbc中,DDL语句不支持绑定变量
  private Object defaultValue;
  private boolean autoIncrement;

  // for doris
  private String aggregateType;
}
