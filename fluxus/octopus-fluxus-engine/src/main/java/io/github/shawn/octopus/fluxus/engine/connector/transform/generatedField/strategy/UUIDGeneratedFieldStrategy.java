package io.github.shawn.octopus.fluxus.engine.connector.transform.generatedField.strategy;

import io.github.shawn.octopus.fluxus.api.common.IdGenerator;

public class UUIDGeneratedFieldStrategy implements GeneratedFiledStrategy {

  @Override
  public String generated() {
    return IdGenerator.uuid();
  }
}
