package com.octopus.kettlex.core.steps.transform.valueMapper;

import com.octopus.kettlex.core.exception.KettleXException;
import com.octopus.kettlex.core.exception.KettleXStepExecuteException;
import com.octopus.kettlex.core.row.Record;
import com.octopus.kettlex.core.row.RecordExchanger;
import com.octopus.kettlex.core.row.column.Column;
import com.octopus.kettlex.core.row.column.FieldType;
import com.octopus.kettlex.core.row.record.DefaultRecord;
import com.octopus.kettlex.core.steps.Transform;
import java.util.Map;
import lombok.AllArgsConstructor;
import lombok.Getter;

@Getter
@AllArgsConstructor
public class ValueMapper implements Transform<ValueMapperConfig, ValueMapperContext> {

  private final ValueMapperConfig stepConfig;
  private final ValueMapperContext stepContext;

  @Override
  public int order() {
    return 0;
  }

  @Override
  public boolean init() throws KettleXException {
    return true;
  }

  @Override
  public void destory() throws KettleXException {}

  @Override
  public void processRow(RecordExchanger recordExchanger) throws KettleXStepExecuteException {
    Record record = recordExchanger.fetch();
    Record targetRecord = new DefaultRecord();
    Map<Object, Object> fieldValueMap = stepConfig.getFieldValueMap();
    String sourceField = stepConfig.getSourceField();
    String targetField = stepConfig.getTargetField();
    FieldType targetFieldType = stepConfig.getTargetFieldType();
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
    recordExchanger.send(targetRecord);
  }
}
