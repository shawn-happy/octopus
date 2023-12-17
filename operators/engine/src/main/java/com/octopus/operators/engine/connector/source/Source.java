package com.octopus.operators.engine.connector.source;

import com.octopus.operators.engine.config.RuntimeEnvironment;

public interface Source<T, Env extends RuntimeEnvironment> {
  T read();

  void setRuntimeEnvironment(Env runtimeEnvironment);
}
