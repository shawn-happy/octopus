package com.octopus.operators.engine.config.sink;

public interface SinkConfig<P extends SinkOptions> {

  SinkType getType();

  String getName();

  P getOptions();

  String getInput();

  Integer getParallelism();

  WriteMode getWriteMode();
}
