package com.octopus.operators.engine.config.step;

public interface SinkConfig<P extends StepOptions> extends StepConfig<P> {

  @Override
  default PluginType getPluginType() {
    return PluginType.SINK;
  }

  String getSourceTable();

  WriteMode getWriteMode();
}
