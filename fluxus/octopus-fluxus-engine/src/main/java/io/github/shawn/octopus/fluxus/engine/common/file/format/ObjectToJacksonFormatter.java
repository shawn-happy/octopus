package io.github.shawn.octopus.fluxus.engine.common.file.format;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;

public interface ObjectToJacksonFormatter {
  JsonNode format(ObjectMapper mapper, JsonNode node, Object value);
}
