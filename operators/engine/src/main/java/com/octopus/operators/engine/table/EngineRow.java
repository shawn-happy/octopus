package com.octopus.operators.engine.table;

import com.octopus.operators.engine.table.type.RowDataType;
import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Getter;
import lombok.NoArgsConstructor;

@Getter
@Builder
@NoArgsConstructor
@AllArgsConstructor
public class EngineRow {

  private String[] fieldNames;
  private Object[] fieldValues;
  private RowDataType[] fieldTypes;

  public EngineRow(int rowSize) {
    this.fieldNames = new String[rowSize];
    this.fieldValues = new Object[rowSize];
    this.fieldTypes = new RowDataType[rowSize];
  }

  public String[] getFieldNames() {
    return fieldNames;
  }

  public Object[] getFieldValues() {
    return fieldValues;
  }

  public RowDataType[] getFieldTypes() {
    return fieldTypes;
  }
}
