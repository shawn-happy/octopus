package com.octopus.kettlex.core.plugin;

public enum PluginType {
  STEP("STEP"),
  ;

  private final String type;

  PluginType(String type) {
    this.type = type;
  }

  public String getType() {
    return type;
  }
}
