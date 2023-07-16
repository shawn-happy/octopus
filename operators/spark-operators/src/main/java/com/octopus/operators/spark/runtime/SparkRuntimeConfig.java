package com.octopus.operators.spark.runtime;

public class SparkRuntimeConfig {

  private String configPath;
  private String executorType;
  private String runtimeMode;
  private String appName;
  private String masterUrl;
  private boolean enableHive;

  public SparkRuntimeConfig() {}

  public String getConfigPath() {
    return configPath;
  }

  public void setConfigPath(String configPath) {
    this.configPath = configPath;
  }

  public String getExecutorType() {
    return executorType;
  }

  public void setExecutorType(String executorType) {
    this.executorType = executorType;
  }

  public String getRuntimeMode() {
    return runtimeMode;
  }

  public void setRuntimeMode(String runtimeMode) {
    this.runtimeMode = runtimeMode;
  }

  public boolean isEnableHive() {
    return enableHive;
  }

  public void setEnableHive(boolean enableHive) {
    this.enableHive = enableHive;
  }

  public String getAppName() {
    return appName;
  }

  public void setAppName(String appName) {
    this.appName = appName;
  }

  public String getMasterUrl() {
    return masterUrl;
  }

  public void setMasterUrl(String masterUrl) {
    this.masterUrl = masterUrl;
  }
}
