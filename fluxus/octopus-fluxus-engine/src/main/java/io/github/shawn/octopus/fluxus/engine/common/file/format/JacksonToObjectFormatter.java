package io.github.shawn.octopus.fluxus.engine.common.file.format;

import com.fasterxml.jackson.databind.JsonNode;

public interface JacksonToObjectFormatter {
  Object format(JsonNode jsonNode);
}
