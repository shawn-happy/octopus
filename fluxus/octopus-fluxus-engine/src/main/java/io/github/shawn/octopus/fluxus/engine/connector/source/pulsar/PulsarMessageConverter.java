package io.github.shawn.octopus.fluxus.engine.connector.source.pulsar;

import com.fasterxml.jackson.core.type.TypeReference;
import io.github.shawn.octopus.fluxus.api.common.JsonUtils;
import io.github.shawn.octopus.fluxus.api.converter.RowRecordConverter;
import io.github.shawn.octopus.fluxus.api.exception.DataWorkflowException;
import io.github.shawn.octopus.fluxus.api.model.type.DataWorkflowFieldType;
import io.github.shawn.octopus.fluxus.engine.model.table.Schema;
import io.github.shawn.octopus.fluxus.engine.model.table.SourceRowRecord;
import java.nio.charset.StandardCharsets;
import java.util.List;
import java.util.Map;
import org.apache.pulsar.client.api.Message;

public class PulsarMessageConverter implements RowRecordConverter<Message<byte[]>> {

  private final List<Schema> schemas;

  public PulsarMessageConverter(List<Schema> schemas) {
    this.schemas = schemas;
  }

  @Override
  public SourceRowRecord convert(Message<byte[]> message) {
    byte[] value = message.getValue();
    String json = new String(value, StandardCharsets.UTF_8);
    Map<String, Object> map =
        JsonUtils.fromJson(json, new TypeReference<Map<String, Object>>() {})
            .orElseThrow(
                () ->
                    new DataWorkflowException(
                        String.format("message parse error. message: [%s]", json)));
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
