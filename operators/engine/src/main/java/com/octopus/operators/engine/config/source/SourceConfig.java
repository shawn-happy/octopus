package com.octopus.operators.engine.config.source;

import org.jetbrains.annotations.NotNull;

public interface SourceConfig<P extends SourceOptions> {

  @NotNull
  SourceType getType();

  @NotNull
  String getName();

  P getOptions();

  @NotNull
  String getOutput();

  Integer getParallelism();
}
