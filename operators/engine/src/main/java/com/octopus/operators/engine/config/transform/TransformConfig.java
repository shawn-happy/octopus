package com.octopus.operators.engine.config.transform;

import java.util.Map;

public interface TransformConfig<P extends TransformOptions> {

  TransformType getType();

  String getName();

  P getOptions();

  Map<String, String> getInputs();

  String getOutput();

  Integer getParallelism();
}
