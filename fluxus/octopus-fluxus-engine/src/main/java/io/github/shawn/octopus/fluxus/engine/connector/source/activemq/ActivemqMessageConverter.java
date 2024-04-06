package io.github.shawn.octopus.fluxus.engine.connector.source.activemq;

import com.fasterxml.jackson.core.type.TypeReference;
import io.github.shawn.octopus.fluxus.api.common.JsonUtils;
import io.github.shawn.octopus.fluxus.api.converter.RowRecordConverter;
import io.github.shawn.octopus.fluxus.api.exception.DataWorkflowException;
import io.github.shawn.octopus.fluxus.api.model.type.DataWorkflowFieldType;
import io.github.shawn.octopus.fluxus.engine.model.table.Schema;
import io.github.shawn.octopus.fluxus.engine.model.table.SourceRowRecord;
import java.util.List;
import java.util.Map;

/** @author zuoyl */
public class ActivemqMessageConverter implements RowRecordConverter<String> {

  private final List<Schema> schemas;

  public ActivemqMessageConverter(List<Schema> schemas) {
    this.schemas = schemas;
  }

  @Override
  public SourceRowRecord convert(String message) {

    Map<String, Object> map =
        JsonUtils.fromJson(message, new TypeReference<Map<String, Object>>() {})
            .orElseThrow(
                () ->
                    new DataWorkflowException(
                        String.format("message parse error. message: [%s]", message)));
    Object[] values = new Object[schemas.size()];
    String[] fieldNames = new String[schemas.size()];
    DataWorkflowFieldType[] dataWorkflowFieldTypes = new DataWorkflowFieldType[schemas.size()];
    for (int i = 0; i < schemas.size(); i++) {
      Schema schema = schemas.get(i);
      String fieldName = schema.getFieldName();
      Object fieldValue =
          map.containsKey(fieldName) ? map.get(fieldName) : schema.getDefaultValue();
      fieldNames[i] = fieldName;
      values[i] = fieldValue;
      dataWorkflowFieldTypes[i] = schema.getFieldType();
    }
    return SourceRowRecord.builder()
        .fieldNames(fieldNames)
        .fieldTypes(dataWorkflowFieldTypes)
        .values(values)
        .build();
  }
}
