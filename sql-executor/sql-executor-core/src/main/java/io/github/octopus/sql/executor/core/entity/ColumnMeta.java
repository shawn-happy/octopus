package io.github.octopus.sql.executor.core.entity;

import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Getter;
import lombok.NoArgsConstructor;

@Getter
@Builder
@NoArgsConstructor
@AllArgsConstructor
public class ColumnMeta {
  private String database;
  // for sqlserver
  private String schema;
  private String table;
  private String column;
  private Object defaultValue;
  private String nullable;
  private String columnType;
  private String dataType;
  private Long length;
  private Integer precision;
  private Integer scale;
  // for mysql
  private Integer timePrecision;
  private String comment;
}
