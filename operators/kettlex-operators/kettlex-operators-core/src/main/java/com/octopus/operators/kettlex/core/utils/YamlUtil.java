package com.octopus.operators.kettlex.core.utils;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.core.json.JsonReadFeature;
import com.fasterxml.jackson.core.type.TypeReference;
import com.fasterxml.jackson.databind.DeserializationFeature;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.dataformat.yaml.YAMLFactory;
import com.fasterxml.jackson.dataformat.yaml.YAMLGenerator;
import java.io.IOException;
import java.io.Reader;
import java.util.Optional;
import java.util.function.Function;
import java.util.function.Supplier;
import lombok.extern.slf4j.Slf4j;

@Slf4j
public class YamlUtil {

  private static final ObjectMapper OM =
      new ObjectMapper(new YAMLFactory().disable(YAMLGenerator.Feature.WRITE_DOC_START_MARKER))
          .findAndRegisterModules()
          .enable(JsonReadFeature.ALLOW_NON_NUMERIC_NUMBERS.mappedFeature())
          .disable(DeserializationFeature.FAIL_ON_UNKNOWN_PROPERTIES);

  /**
   * Convert an object to Yaml string
   *
   * @param object the object to be converted
   * @return Yaml string, or null if any error happens
   */
  public static Optional<String> toYaml(Object object) {
    try {
      return Optional.of(OM.writeValueAsString(object));
    } catch (JsonProcessingException e) {
      log.error("error on serialize", e);
      throw new RuntimeException(e);
    }
  }
  /**
   * Convert an object to Yaml string
   *
   * @param object the object to be converted
   * @param errorMessageConverter error message consumer, if there is error message
   * @return Yaml string
   */
  public static String toYaml(
      Object object, Function<Throwable, RuntimeException> errorMessageConverter) {
    try {
      return OM.writeValueAsString(object);
    } catch (JsonProcessingException e) {
      log.error("error on serialize", e);
      throw errorMessageConverter.apply(e);
    }
  }

  /**
   * Convert Yaml string to {@code type}
   *
   * <p>Note that if the type is List or Map, please check {@code fromYaml} with TypeReference
   *
   * @param Yaml Yaml string
   * @param type the type to convert the Yaml to
   * @param <T> the type to convert the Yaml to
   * @return The object converted from Yaml string, or null if any error happens.
   */
  public static <T> Optional<T> fromYaml(String Yaml, Class<T> type) {
    try {
      return Optional.of(OM.readValue(Yaml, type));
    } catch (IOException e) {
      log.error("error on deserialize", e);
      throw new RuntimeException(e);
    }
  }

  /**
   * It would be a bit trivial to convert Yaml to List/Map of objects.
   *
   * <p>For example:
   *
   * <pre>
   * List<SimpleClass> = YamlUtil.fromYaml(Yaml, List.class);
   * </pre>
   *
   * won't work because Jackson don't know what the exact type to convert to. You should however:
   *
   * <pre>
   * List<SimpleClass> simpleClass = YamlUtil.fromYaml(
   *         simpleListYaml,
   *         new TypeReference<List<SimpleClass>>() {});
   * </pre>
   *
   * By giving TypeReference, YamlUtil know how to convert the types.
   *
   * @param Yaml Yaml string
   * @param type the type to convert the Yaml to
   * @param <T> the type to convert the Yaml to
   * @return The object converted from Yaml string, or null if any error happens.
   */
  public static <T> Optional<T> fromYaml(String Yaml, TypeReference<T> type) {
    try {
      return Optional.of(OM.readValue(Yaml, type));
    } catch (IOException e) {
      log.error("error on deserialize for Yaml {}", Yaml, e);
      throw new RuntimeException(e);
    }
  }

  public static <T> T fromYaml(
      String Yaml,
      TypeReference<T> type,
      Function<Throwable, RuntimeException> errorMessageConverter) {
    try {
      return OM.readValue(Yaml, type);
    } catch (IOException e) {
      log.error("error on deserialize for Yaml {}", Yaml, e);
      throw errorMessageConverter.apply(e);
    }
  }

  public static <T> T fromYaml(
      String Yaml, TypeReference<T> type, Supplier<RuntimeException> runtimeExceptionSupplier) {
    try {
      return OM.readValue(Yaml, type);
    } catch (IOException e) {
      log.error("error on deserialize for Yaml {}", Yaml, e);
      throw runtimeExceptionSupplier.get();
    }
  }

  public static <T> Optional<T> fromYaml(Reader Yaml, Class<T> type) {
    try {
      return Optional.of(OM.readValue(Yaml, type));
    } catch (IOException e) {
      log.error("error on deserialize", e);
      throw new RuntimeException(e);
    }
  }

  public static <T> Optional<T> fromYaml(byte[] Yaml, Class<T> type) {
    try {
      return Optional.of(OM.readValue(Yaml, type));
    } catch (IOException e) {
      log.error("error on deserialize", e);
      throw new RuntimeException(e);
    }
  }

  public static <T> Optional<T> fromYaml(Reader Yaml, TypeReference<T> type) {
    try {
      return Optional.of(OM.readValue(Yaml, type));
    } catch (IOException e) {
      log.error("error on deserialize", e);
      throw new RuntimeException(e);
    }
  }

  public static ObjectMapper getObjectMapper() {
    return OM;
  }

  /**
   * Convert Yaml to Jackson Tree Model
   *
   * @param Yaml Yaml string
   * @return the Yaml Tree, or null if any error happens
   */
  public static Optional<JsonNode> readTree(String Yaml) {
    try {
      return Optional.of(OM.readTree(Yaml));
    } catch (IOException e) {
      log.error("error on deserialize", e);
      throw new RuntimeException(e);
    }
  }

  public static <T> Optional<T> convertValue(Object fromValue, Class<T> type) {
    return Optional.ofNullable(OM.convertValue(fromValue, type));
  }

  public static <T> Optional<T> convertValue(Object fromValue, TypeReference<T> type) {
    return Optional.ofNullable(OM.convertValue(fromValue, type));
  }

  public static Optional<JsonNode> toYamlNode(String Yaml) {
    try {
      return Optional.ofNullable(OM.readTree(Yaml));
    } catch (IOException e) {
      log.error("error on deserialize", e);
      throw new RuntimeException(e);
    }
  }

  public static Optional<JsonNode> toYamlNode(byte[] Yaml) {
    try {
      return Optional.ofNullable(OM.readTree(Yaml));
    } catch (IOException e) {
      log.error("error on deserialize", e);
      throw new RuntimeException(e);
    }
  }
}
