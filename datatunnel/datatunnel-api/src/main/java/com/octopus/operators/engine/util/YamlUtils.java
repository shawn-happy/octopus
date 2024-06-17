package com.octopus.operators.engine.util;

import com.fasterxml.jackson.core.type.TypeReference;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ArrayNode;
import com.fasterxml.jackson.databind.node.ObjectNode;
import com.fasterxml.jackson.dataformat.yaml.YAMLFactory;
import com.fasterxml.jackson.dataformat.yaml.YAMLGenerator;
import com.octopus.operators.engine.exception.EngineException;
import java.util.ArrayList;
import java.util.List;
import java.util.function.Function;
import lombok.extern.slf4j.Slf4j;

@Slf4j
public final class YamlUtils {

  private static final ObjectMapper OM_YAML =
      new ObjectMapper(new YAMLFactory().disable(YAMLGenerator.Feature.WRITE_DOC_START_MARKER));

  private YamlUtils() {}

  public static String toYaml(Object obj) {
    return toYaml(obj, t -> new EngineException("write yaml error.", t));
  }

  public static String toYaml(Object obj, Function<Throwable, RuntimeException> exceptionFunction) {
    try {
      return OM_YAML.writeValueAsString(obj);
    } catch (Exception e) {
      log.error("write yaml error.", e);
      throw exceptionFunction.apply(e);
    }
  }

  public static <T> T fromYaml(String yaml, Class<T> tClass) {
    try {
      log.info("yaml content: {}", yaml);
      return OM_YAML.readValue(yaml, tClass);
    } catch (Exception e) {
      log.error("read yaml error.", e);
      throw new EngineException("read yaml error", e);
    }
  }

  public static <T> T fromYaml(String yaml, TypeReference<T> typeReference) {
    try {
      log.info("yaml content: {}", yaml);
      return OM_YAML.readValue(yaml, typeReference);
    } catch (Exception e) {
      log.error("read yaml error.", e);
      throw new EngineException("read yaml error", e);
    }
  }

  public static <T> T fromYaml(JsonNode jsonNode, TypeReference<T> typeReference) {
    try {
      log.info("yaml content: {}", jsonNode.toPrettyString());
      return OM_YAML.readValue(jsonNode.toPrettyString(), typeReference);
    } catch (Exception e) {
      log.error("read yaml error.", e);
      throw new EngineException("read yaml error", e);
    }
  }

  public static JsonNode toJsonNode(String yaml) {
    try {
      log.info("yaml content: {}", yaml);
      return OM_YAML.readTree(yaml);
    } catch (Exception e) {
      log.error("read yaml error.", e);
      throw new EngineException("read yaml error", e);
    }
  }

  public static ObjectNode createObjectNode() {
    return OM_YAML.createObjectNode();
  }

  public static boolean isBlank(JsonNode jsonNode) {
    return jsonNode == null || jsonNode.isEmpty() || jsonNode.isNull() || jsonNode.isMissingNode();
  }

  public static boolean isNotBlank(JsonNode jsonNode) {
    return !isBlank(jsonNode);
  }

  public static String getTextValue(JsonNode jsonNode, String key) {
    if (isBlank(jsonNode)) {
      return null;
    }
    JsonNode valueNode = jsonNode.path(key);
    if (isBlank(valueNode)) {
      return null;
    }
    return valueNode.textValue();
  }

  public static List<String> getStringArray(JsonNode jsonNode, String key) {
    if (isBlank(jsonNode)) {
      return null;
    }
    JsonNode valueNode = jsonNode.path(key);
    if (isBlank(valueNode)) {
      return null;
    }
    List<String> values = null;
    if (valueNode.isArray()) {
      ArrayNode arrayNode = (ArrayNode) valueNode;
      values = new ArrayList<>(arrayNode.size());
      for (JsonNode node : arrayNode) {
        values.add(node.asText());
      }
    }
    return values;
  }
}
