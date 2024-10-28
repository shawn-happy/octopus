package io.github.octopus.sql.executor.core.model.metadata;

import io.github.octopus.sql.executor.core.model.schema.FieldType;
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
  private String schemaName;
  private String tableName;
  private String columnName;
  private Object defaultValue;
  private boolean nullable;
  private FieldType fieldType;
  private Integer precision;
  private Integer scale;
  private String comment;
}
