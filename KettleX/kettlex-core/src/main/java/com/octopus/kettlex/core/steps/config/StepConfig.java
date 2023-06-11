package com.octopus.kettlex.core.steps.config;

import com.fasterxml.jackson.core.type.TypeReference;
import com.octopus.kettlex.core.exception.KettleXJSONException;
import com.octopus.kettlex.core.exception.KettleXStepConfigException;
import com.octopus.kettlex.core.steps.config.StepConfig.StepOptions;
import com.octopus.kettlex.core.utils.JsonUtil;
import java.util.Collections;
import java.util.Map;
import java.util.Objects;
import org.apache.commons.lang3.StringUtils;

public interface StepConfig<T extends StepOptions> {

  String getId();

  String getName();

  T getOptions();

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
                  .orElseThrow(() -> new KettleXJSONException("parse Options error")),
              new TypeReference<Map<String, String>>() {})
          .orElse(Collections.emptyMap());
    }

    void verify();
  }
}
