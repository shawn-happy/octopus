package io.github.shawn.octopus.fluxus.engine.connector.transform.generatedField.strategy;

import io.github.shawn.octopus.fluxus.engine.connector.transform.generatedField.SnowFlakeUtil;

public class SnowIdGeneratedFiledStrategy implements GeneratedFiledStrategy {

  @Override
  public String generated() {
    return SnowFlakeUtil.getNextId();
  }
}
