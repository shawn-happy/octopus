package com.octopus.operators.kettlex.core.steps.config;

import com.fasterxml.jackson.core.type.TypeReference;
import com.fasterxml.jackson.databind.JsonNode;
import com.octopus.operators.kettlex.core.exception.KettleXParseException;
import com.octopus.operators.kettlex.core.exception.KettleXStepConfigException;
import com.octopus.operators.kettlex.core.steps.config.StepConfig.StepOptions;
import com.octopus.operators.kettlex.core.utils.JsonUtil;
import com.octopus.operators.kettlex.core.utils.YamlUtil;
import java.util.Collections;
import java.util.Map;
import java.util.Objects;
import org.apache.commons.lang3.StringUtils;

public interface StepConfig<T extends StepOptions> {

  String getId();

  String getName();

  String getType();

  T getOptions();

  default void loadYaml(String yaml) {
    loadYaml(YamlUtil.toYamlNode(yaml).orElse(null));
  }

  void loadYaml(JsonNode jsonNode);

  default void verify() {
    if (StringUtils.isBlank(getName())) {
      throw new KettleXStepConfigException("Step name cannot be null");
    }
    if (!Objects.isNull(getOptions())) {
      getOptions().verify();
    }
  }

  interface StepOptions {
    default Map<String, String> getOptions() {
      return JsonUtil.fromJson(
              JsonUtil.toJson(this)
                  .orElseThrow(() -> new KettleXParseException("parse Options error")),
              new TypeReference<Map<String, String>>() {})
          .orElse(Collections.emptyMap());
    }

    void verify();

    default void loadYaml(String yaml) {
      loadYaml(YamlUtil.toYamlNode(yaml).orElse(null));
    }

    void loadYaml(JsonNode jsonNode);
  }
}
