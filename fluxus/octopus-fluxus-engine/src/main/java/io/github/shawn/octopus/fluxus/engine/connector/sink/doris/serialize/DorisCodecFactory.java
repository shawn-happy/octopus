package io.github.shawn.octopus.fluxus.engine.connector.sink.doris.serialize;

import io.github.shawn.octopus.fluxus.api.exception.DataWorkflowException;
import io.github.shawn.octopus.fluxus.api.model.type.DataWorkflowFieldType;
import io.github.shawn.octopus.fluxus.engine.connector.sink.doris.DorisConstants;
import io.github.shawn.octopus.fluxus.engine.connector.sink.doris.DorisSinkConfig;
import io.github.shawn.octopus.fluxus.engine.connector.sink.doris.StreamLoadFormat;

public class DorisCodecFactory {

  public static DorisCodec createCodec(
      DorisSinkConfig.DorisSinkOptions options,
      String[] fieldNames,
      DataWorkflowFieldType[] fieldTypes) {
    StreamLoadFormat streamLoadFormat = options.getStreamLoadFormat();
    switch (streamLoadFormat) {
      case JSON:
        return new DorisJsonCodec(fieldNames, fieldTypes);
      case CSV:
        return new DorisCsvCodec(
            fieldNames,
            fieldTypes,
            String.valueOf(options.getExtraOptions().get(DorisConstants.FIELD_DELIMITER_KEY)));
    }
    throw new DataWorkflowException(
        "Failed to create row serializer, unsupported `format` from stream load properties.");
  }
}
