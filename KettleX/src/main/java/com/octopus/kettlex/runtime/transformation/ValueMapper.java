package com.octopus.kettlex.runtime.transformation;

import com.octopus.kettlex.core.exception.KettleXStepExecuteException;
import com.octopus.kettlex.core.row.Record;
import com.octopus.kettlex.core.row.column.Column;
import com.octopus.kettlex.core.row.column.FieldType;
import com.octopus.kettlex.core.row.record.DefaultRecord;
import com.octopus.kettlex.core.steps.BaseStep;
import com.octopus.kettlex.core.steps.Transform;
import com.octopus.kettlex.model.transformation.ValueMapperConfig;
import com.octopus.kettlex.model.transformation.ValueMapperConfig.ValueMapperOptions;
import com.octopus.kettlex.runtime.TaskGroup;
import java.util.Map;
import lombok.Getter;

@Getter
public class ValueMapper extends BaseStep<ValueMapperConfig>
    implements Transform<ValueMapperConfig> {

  private final ValueMapperConfig stepConfig;

  public ValueMapper(ValueMapperConfig stepConfig, TaskGroup taskGroup) {
    super(stepConfig, taskGroup);
    this.stepConfig = stepConfig;
  }

  @Override
  public void processRow() throws KettleXStepExecuteException {
    Record record = getRow();
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
    putRow(record);
  }
}
