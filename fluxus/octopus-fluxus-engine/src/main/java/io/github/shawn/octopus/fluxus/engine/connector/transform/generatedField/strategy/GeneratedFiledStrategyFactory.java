package io.github.shawn.octopus.data.fluxus.engine.connector.transform.generatedField.strategy;

import io.github.shawn.octopus.data.fluxus.engine.connector.transform.generatedField.GenerateType;
import java.util.HashMap;
import java.util.Map;

public class GeneratedFiledStrategyFactory {
  private static final Map<String, GeneratedFiledStrategy> strategies = new HashMap<>();

  static {
    strategies.put(GenerateType.CURRENTTIME.getType(), new CurrentTimeGeneratedFiledStrategy());
    strategies.put(GenerateType.SNOWID.getType(), new UUIDGeneratedFieldStrategy());
    strategies.put(GenerateType.UUID.getType(), new SnowIdGeneratedFiledStrategy());
  }

  public static GeneratedFiledStrategy getStrategy(String type) {
    if (type == null || type.isEmpty()) {
      throw new IllegalArgumentException("type should not be empty.");
    }
    return strategies.get(type.toUpperCase());
  }
}
