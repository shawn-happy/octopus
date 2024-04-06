package io.github.shawn.octopus.fluxus.engine.common.file.serialization;

import com.fasterxml.jackson.core.json.JsonReadFeature;
import com.fasterxml.jackson.databind.DeserializationFeature;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ObjectNode;
import io.github.shawn.octopus.fluxus.api.exception.DataWorkflowException;
import io.github.shawn.octopus.fluxus.api.model.table.RowRecord;
import io.github.shawn.octopus.fluxus.api.serialization.SerializationSchema;
import io.github.shawn.octopus.fluxus.engine.common.file.format.JacksonFormatters;
import io.github.shawn.octopus.fluxus.engine.common.file.format.ObjectToJacksonFormatter;
import io.github.shawn.octopus.fluxus.engine.model.type.RowFieldType;
import lombok.Getter;

public class JsonSerializationSchema implements SerializationSchema {

  /** Reusable object node. */
  private transient ObjectNode node;

  /** Object mapper that is used to create output JSON objects. */
  @Getter
  private final ObjectMapper objectMapper =
      new ObjectMapper()
          .findAndRegisterModules()
          .enable(JsonReadFeature.ALLOW_NON_NUMERIC_NUMBERS.mappedFeature())
          .enable(DeserializationFeature.USE_BIG_DECIMAL_FOR_FLOATS)
          .disable(DeserializationFeature.FAIL_ON_UNKNOWN_PROPERTIES)
          .configure(JsonReadFeature.ALLOW_UNESCAPED_CONTROL_CHARS.mappedFeature(), true);

  private final ObjectToJacksonFormatter jacksonFormatter;

  public JsonSerializationSchema(RowFieldType rowType) {
    this.jacksonFormatter = JacksonFormatters.createObjectToJacksonFormatter(rowType);
  }

  @Override
  public byte[] serialize(RowRecord record) {
    if (node == null) {
      node = objectMapper.createObjectNode();
    }

    try {
      jacksonFormatter.format(objectMapper, node, record);
      return objectMapper.writeValueAsBytes(node);
    } catch (Throwable e) {
      throw new DataWorkflowException(String.format("Failed to deserialize JSON '%s'.", record), e);
    }
  }
}
