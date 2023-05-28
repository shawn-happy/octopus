package com.octopus.kettlex.core.steps.transform.valueMapper;

import com.octopus.kettlex.core.monitor.ExecutionStatus;
import com.octopus.kettlex.core.steps.StepContext;
import java.util.HashMap;
import java.util.Map;

public class ValueMapperContext implements StepContext {

  private Map<String, String> fieldMapper = new HashMap<>();

  @Override
  public void markStatus(ExecutionStatus status) {}

  public void put(String sourceValue, String targetValue) {
    fieldMapper.computeIfAbsent(sourceValue, (v) -> targetValue);
  }

  public String get(String sourceValue) {
    return fieldMapper.get(sourceValue);
  }
}
