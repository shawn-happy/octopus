package io.github.shawn.octopus.fluxus.engine.connector.sink.doris.serialize;

import io.github.shawn.octopus.fluxus.api.common.JsonUtils;
import io.github.shawn.octopus.fluxus.api.model.type.DataWorkflowFieldType;
import io.github.shawn.octopus.fluxus.engine.common.utils.InternalRowRecordConverters;
import java.util.HashMap;
import java.util.Map;

public class DorisJsonCodec extends DorisCodec {

  public DorisJsonCodec(String[] fieldNames, DataWorkflowFieldType[] fieldTypes) {
    super(fieldNames, fieldTypes);
  }

  @Override
  public String codec(Object[] values) {
    Map<String, Object> rowMap = new HashMap<>(fieldNames.length);
    for (int i = 0; i < fieldNames.length; i++) {
      String value = InternalRowRecordConverters.convertToString(fieldTypes[i], values[i]);
      rowMap.put(fieldNames[i], value);
    }
    return JsonUtils.toJson(rowMap).orElse(null);
  }
}
