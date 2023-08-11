package com.octopus.operators.flink.runtime;

import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.EnvironmentSettings;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;

public class FlinkRuntimeEnvironment {
  private StreamExecutionEnvironment environment;
  private StreamTableEnvironment tableEnvironment;

  public static volatile FlinkRuntimeEnvironment flinkRuntimeEnvironment;

  public static FlinkRuntimeEnvironment getFlinkRuntimeEnvironment() {
    if (flinkRuntimeEnvironment == null) {
      synchronized (FlinkRuntimeEnvironment.class) {
        if (flinkRuntimeEnvironment == null) {
          flinkRuntimeEnvironment = new FlinkRuntimeEnvironment();
        }
      }
    }
    return flinkRuntimeEnvironment;
  }

  private FlinkRuntimeEnvironment() {
    createStreamEnvironment();
    createStreamTableEnvironment();
  }

  public StreamExecutionEnvironment getStreamExecutionEnvironment() {
    return environment;
  }

  public StreamTableEnvironment getStreamTableEnvironment() {
    return tableEnvironment;
  }

  private void createStreamTableEnvironment() {
    EnvironmentSettings environmentSettings =
        EnvironmentSettings.newInstance().inBatchMode().build();
    tableEnvironment =
        StreamTableEnvironment.create(getStreamExecutionEnvironment(), environmentSettings);
  }

  private void createStreamEnvironment() {
    environment = StreamExecutionEnvironment.getExecutionEnvironment();
  }
}
