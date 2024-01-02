package com.octopus.operators.engine.config.transform;

import java.util.List;

public interface TransformConfig<P extends TransformOptions> {

  TransformType getType();

  String getName();

  P getOptions();

  List<String> getInputs();

  String getOutput();

  Integer getParallelism();
}
