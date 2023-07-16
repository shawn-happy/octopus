package com.octopus.operators.kettlex.builtin.rowgenerator;

import com.octopus.operators.kettlex.core.row.column.FieldType;
import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Getter;
import lombok.NoArgsConstructor;

@Getter
@Builder
@NoArgsConstructor
@AllArgsConstructor
public class Field {

  private String name;
  private String format;
  private Integer length;
  private FieldType fieldType;
  private Object value;
}
