package io.github.octopus.datos.centro.common.utils;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.core.json.JsonReadFeature;
import com.fasterxml.jackson.core.type.TypeReference;
import com.fasterxml.jackson.databind.DeserializationFeature;
import com.fasterxml.jackson.databind.ObjectMapper;
import java.util.function.Function;
import java.util.function.Supplier;
import lombok.extern.slf4j.Slf4j;

@Slf4j
public class JsonUtil {

  private static final ObjectMapper OM =
      new ObjectMapper()
          .findAndRegisterModules()
          .enable(JsonReadFeature.ALLOW_NON_NUMERIC_NUMBERS.mappedFeature())
          .disable(DeserializationFeature.FAIL_ON_UNKNOWN_PROPERTIES);

  private JsonUtil() {}

  public static <T> T fromJson(String json, Class<T> tClass) throws JsonProcessingException {
    log.info("json: {}", json);
    return OM.readValue(json, tClass);
  }

  public static <T> T fromJson(String json, TypeReference<T> reference)
      throws JsonProcessingException {
    log.info("json: {}", json);
    return OM.readValue(json, reference);
  }

  public static <T> T fromJson(
      String json, Class<T> tClass, Function<Throwable, RuntimeException> exceptionFunction) {
    log.info("json: {}", json);
    try {
      return OM.readValue(json, tClass);
    } catch (Throwable e) {
      throw exceptionFunction.apply(e);
    }
  }

  public static <T> T fromJson(
      String json, Class<T> tClass, Supplier<Throwable> throwableSupplier) {
    log.info("json: {}", json);
    try {
      return OM.readValue(json, tClass);
    } catch (Throwable e) {
      throw new RuntimeException(throwableSupplier.get());
    }
  }

  public static <T> T fromJson(
      String json,
      TypeReference<T> reference,
      Function<Throwable, RuntimeException> exceptionFunction) {
    log.info("json: {}", json);
    try {
      return OM.readValue(json, reference);
    } catch (Throwable e) {
      throw exceptionFunction.apply(e);
    }
  }

  public static String toJson(Object obj) throws JsonProcessingException {
    return OM.writeValueAsString(obj);
  }

  public static String toJson(Object obj, Function<Throwable, RuntimeException> exceptionFunction)
      throws JsonProcessingException {
    try {
      return OM.writeValueAsString(obj);
    } catch (Throwable e) {
      throw exceptionFunction.apply(e);
    }
  }
}
