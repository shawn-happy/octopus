package com.octopus.operators.engine.table.catalog;

import com.octopus.operators.engine.table.type.RowDataType;
import com.octopus.operators.engine.table.type.RowDataTypeParse;
import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Getter;
import lombok.NoArgsConstructor;
import org.apache.commons.lang3.StringUtils;
import org.jetbrains.annotations.NotNull;

@Getter
@Builder
@NoArgsConstructor
@AllArgsConstructor
public class Column {
  private String name;
  private String fieldType;
  private String alias;
  private boolean nullable;
  private boolean primaryKey;
  private Object defaultValue;
  private transient RowDataType rowDataType;


}
