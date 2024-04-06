package io.github.shawn.octopus.fluxus.engine.connector.transform.generatedField;

public enum GenerateType {
  CURRENTTIME("CURRENTTIME"),
  UUID("UUID"),
  SNOWID("SNOWID");

  private final String type;

  GenerateType(String type) {
    this.type = type;
  }

  public String getType() {
    return type;
  }
}
