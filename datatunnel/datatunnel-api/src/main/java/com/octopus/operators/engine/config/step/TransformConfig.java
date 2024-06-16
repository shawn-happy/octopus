package com.octopus.operators.engine.config.step;

import java.util.List;

public interface TransformConfig<P extends StepOptions> extends StepConfig<P> {

  @Override
  default PluginType getPluginType() {
    return PluginType.TRANSFORM;
  }

  List<String> getSourceTables();

  String getResultTable();
}
