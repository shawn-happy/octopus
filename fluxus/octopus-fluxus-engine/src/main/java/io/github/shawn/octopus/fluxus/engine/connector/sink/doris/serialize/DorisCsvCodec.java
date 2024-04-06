package io.github.shawn.octopus.fluxus.engine.connector.sink.doris.serialize;

import io.github.shawn.octopus.fluxus.api.model.type.DataWorkflowFieldType;
import io.github.shawn.octopus.fluxus.engine.common.utils.InternalRowRecordConverters;
import java.util.StringJoiner;

public class DorisCsvCodec extends DorisCodec {

  private final String fieldDelimiter;
  private static final String NULL_VALUE = "\\N";

  protected DorisCsvCodec(
      String[] fieldNames, DataWorkflowFieldType[] fieldTypes, String fieldDelimiter) {
    super(fieldNames, fieldTypes);
    this.fieldDelimiter = fieldDelimiter;
  }

  @Override
  public String codec(Object[] values) {
    StringJoiner joiner = new StringJoiner(fieldDelimiter);
    for (int i = 0; i < fieldNames.length; i++) {
      Object field = InternalRowRecordConverters.convertToString(fieldTypes[i], values[i]);
      String value = field != null ? field.toString() : NULL_VALUE;
      joiner.add(value);
    }

    return joiner.toString();
  }
}
