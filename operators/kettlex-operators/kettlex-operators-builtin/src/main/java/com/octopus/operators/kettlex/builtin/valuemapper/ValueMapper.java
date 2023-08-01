package com.octopus.operators.kettlex.builtin.valuemapper;

import com.octopus.operators.kettlex.builtin.valuemapper.ValueMapperConfig.ValueMapperOptions;
import com.octopus.operators.kettlex.core.exception.KettleXException;
import com.octopus.operators.kettlex.core.exception.KettleXStepExecuteException;
import com.octopus.operators.kettlex.core.row.Record;
import com.octopus.operators.kettlex.core.row.column.Column;
import com.octopus.operators.kettlex.core.row.column.FieldType;
import com.octopus.operators.kettlex.core.row.record.DefaultRecord;
import com.octopus.operators.kettlex.core.steps.BaseTransform;
import java.util.Map;

public class ValueMapper extends BaseTransform<ValueMapperConfig> {

  private ValueMapperConfig stepConfig;

  public ValueMapper() {}

  @Override
  protected void doInit(ValueMapperConfig stepConfig) throws KettleXException {
    this.stepConfig = stepConfig;
  }

  @Override
  protected Record processRecord(Record record) throws KettleXStepExecuteException {
    Record targetRecord = new DefaultRecord();
    ValueMapperOptions valueMapperOptions = stepConfig.getOptions();
    try {
      Map<Object, Object> fieldValueMap = valueMapperOptions.getFieldValueMap();
      String sourceField = valueMapperOptions.getSourceField();
      String targetField = valueMapperOptions.getTargetField();
      FieldType targetFieldType = valueMapperOptions.getTargetFieldType();
      for (int i = 0; i < record.getColumnNumber(); i++) {
        Column column = record.getColumn(i);
        Object sourceValue = column.getRawData();
        String sourceFieldName = column.getName();
        String targetFieldName = sourceFieldName;
        Object targetValue = sourceValue;
        FieldType type = column.getType();
        if (sourceFieldName.equals(sourceField)) {
          targetValue = fieldValueMap.getOrDefault(sourceValue, sourceValue);
          targetFieldName = targetField;
          type = targetFieldType;
        }
        Column targetColumn =
            Column.builder().name(targetFieldName).rawData(targetValue).type(type).build();
        targetRecord.addColumn(targetColumn);
      }
    } catch (Exception e) {
      throw new KettleXStepExecuteException(e);
    }
    return targetRecord;
  }
}
