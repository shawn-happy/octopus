package io.github.shawn.octopus.fluxus.api.common;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.core.json.JsonReadFeature;
import com.fasterxml.jackson.core.type.TypeReference;
import com.fasterxml.jackson.databind.DeserializationConfig;
import com.fasterxml.jackson.databind.DeserializationFeature;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.SerializationConfig;
import com.fasterxml.jackson.databind.jsontype.NamedType;
import io.github.shawn.octopus.fluxus.api.exception.DataWorkflowException;
import java.io.IOException;
import java.io.Reader;
import java.util.Optional;
import java.util.function.Function;
import lombok.extern.slf4j.Slf4j;

@Slf4j
public class JsonUtils {
  private static final ObjectMapper OM =
      new ObjectMapper()
          .findAndRegisterModules()
          .enable(JsonReadFeature.ALLOW_NON_NUMERIC_NUMBERS.mappedFeature())
          .disable(DeserializationFeature.FAIL_ON_UNKNOWN_PROPERTIES);

  private static final SerializationConfig SC = OM.getSerializationConfig();
  private static final DeserializationConfig DC = OM.getDeserializationConfig();

  public static void registerSubTypes(String name, Class<?> subType) {
    NamedType namedType = new NamedType(subType, name);
    SC.getSubtypeResolver().registerSubtypes(namedType);
    DC.getSubtypeResolver().registerSubtypes(namedType);
  }

  /**
   * Convert an object to JSON string
   *
   * @param object the object to be converted
   * @return JSON string, or null if any error happens
   */
  public static Optional<String> toJson(Object object) {
    try {
      return Optional.of(OM.writeValueAsString(object));
    } catch (JsonProcessingException e) {
      log.error("error on serialize", e);
      throw new DataWorkflowException(e);
    }
  }

  public static String toJsonString(Object object) {
    try {
      return OM.writeValueAsString(object);
    } catch (JsonProcessingException e) {
      log.error("error on serialize", e);
      throw new DataWorkflowException(e);
    }
  }
  /**
   * Convert an object to JSON string
   *
   * @param object the object to be converted
   * @param errorMessageConverter error message consumer, if there is error message
   * @return JSON string
   */
  public static String toJson(
      Object object, Function<Throwable, RuntimeException> errorMessageConverter) {
    try {
      return OM.writeValueAsString(object);
    } catch (JsonProcessingException e) {
      log.error("error on serialize", e);
      throw errorMessageConverter.apply(e);
    }
  }

  /**
   * Convert JSON string to {@code type}
   *
   * <p>Note that if the type is List or Map, please check {@code fromJson} with TypeReference
   *
   * @param json json string
   * @param type the type to convert the json to
   * @param <T> the type to convert the json to
   * @return The object converted from json string, or null if any error happens.
   */
  public static <T> Optional<T> fromJson(String json, Class<T> type) {
    try {
      return Optional.of(OM.readValue(json, type));
    } catch (IOException e) {
      log.error("error on deserialize", e);
      throw new DataWorkflowException(e);
    }
  }

  public static <T> T readValue(String json, Class<T> type) throws IOException {
    return OM.readValue(json, type);
  }

  /**
   * It would be a bit trivial to convert JSON to List/Map of objects.
   *
   * <p>For example:
   *
   * <pre>
   * List<SimpleClass> = JsonUtil.fromJson(json, List.class);
   * </pre>
   *
   * won't work because Jackson don't know what the exact type to convert to. You should however:
   *
   * <pre>
   * List<SimpleClass> simpleClass = JsonUtil.fromJson(
   *         simpleListJSON,
   *         new TypeReference<List<SimpleClass>>() {});
   * </pre>
   *
   * By giving TypeReference, JsonUtil know how to convert the types.
   *
   * @param json json string
   * @param type the type to convert the json to
   * @param <T> the type to convert the json to
   * @return The object converted from json string, or null if any error happens.
   */
  public static <T> Optional<T> fromJson(String json, TypeReference<T> type) {
    try {
      return Optional.of(OM.readValue(json, type));
    } catch (IOException e) {
      log.error("error on deserialize for json [{}]", json, e);
      throw new DataWorkflowException(e);
    }
  }

  public static <T> T fromJson(
      String json,
      TypeReference<T> type,
      Function<Throwable, RuntimeException> errorMessageConverter) {
    try {
      return OM.readValue(json, type);
    } catch (IOException e) {
      throw errorMessageConverter.apply(e);
    }
  }

  public static <T> Optional<T> fromJson(Reader json, Class<T> type) {
    try {
      return Optional.of(OM.readValue(json, type));
    } catch (IOException e) {
      log.error("error on deserialize", e);
      throw new DataWorkflowException(e);
    }
  }

  public static <T> Optional<T> fromJson(Reader json, TypeReference<T> type) {
    try {
      return Optional.of(OM.readValue(json, type));
    } catch (IOException e) {
      log.error("error on deserialize", e);
      throw new DataWorkflowException(e);
    }
  }

  public static ObjectMapper getObjectMapper() {
    return OM;
  }

  /**
   * Convert json to Jackson Tree Model
   *
   * @param json json string
   * @return the JSON Tree, or null if any error happens
   */
  public static Optional<JsonNode> readTree(String json) {
    try {
      return Optional.of(OM.readTree(json));
    } catch (IOException e) {
      log.error("error on deserialize", e);
      throw new DataWorkflowException(e);
    }
  }

  public static <T> Optional<T> convertValue(Object fromValue, Class<T> type) {
    return Optional.ofNullable(OM.convertValue(fromValue, type));
  }

  public static <T> Optional<T> convertValue(Object fromValue, TypeReference<T> type) {
    return Optional.ofNullable(OM.convertValue(fromValue, type));
  }

  public static Optional<JsonNode> toJsonNode(String json) {
    try {
      return Optional.ofNullable(OM.readTree(json));
    } catch (IOException e) {
      log.error("error on deserialize", e);
      throw new DataWorkflowException(e);
    }
  }

  public static boolean isNull(JsonNode jsonNode) {
    return jsonNode == null || jsonNode.isNull() || jsonNode.isMissingNode();
  }

  public static boolean isNotNull(JsonNode jsonNode) {
    return jsonNode != null && !jsonNode.isNull() && !jsonNode.isMissingNode();
  }
}
