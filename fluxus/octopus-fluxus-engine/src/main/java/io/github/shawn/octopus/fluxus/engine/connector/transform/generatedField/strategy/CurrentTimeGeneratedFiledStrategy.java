package io.github.shawn.octopus.fluxus.engine.connector.transform.generatedField.strategy;

public class CurrentTimeGeneratedFiledStrategy implements GeneratedFiledStrategy {

  @Override
  public String generated() {
    return String.valueOf(System.currentTimeMillis());
  }
}
