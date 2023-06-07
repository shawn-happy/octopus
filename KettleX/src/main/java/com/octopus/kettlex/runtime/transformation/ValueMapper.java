package com.octopus.kettlex.runtime.transformation;

import com.octopus.kettlex.core.row.Record;
import com.octopus.kettlex.core.row.column.Column;
import com.octopus.kettlex.core.row.column.FieldType;
import com.octopus.kettlex.core.row.record.DefaultRecord;
import com.octopus.kettlex.core.steps.BaseTransform;
import com.octopus.kettlex.model.transformation.ValueMapperConfig;
import com.octopus.kettlex.model.transformation.ValueMapperConfig.ValueMapperOptions;
import java.util.Map;
import lombok.Getter;

@Getter
public class ValueMapper extends BaseTransform<ValueMapperConfig> {

  private final ValueMapperConfig stepConfig;

  public ValueMapper(ValueMapperConfig stepConfig) {
    super(stepConfig);
    this.stepConfig = stepConfig;
  }

  @Override
  protected Record processRecord(Record record) {
    Record targetRecord = new DefaultRecord();
    ValueMapperOptions valueMapperOptions = stepConfig.getOptions();
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
    return targetRecord;
  }
}
