package com.octopus.operators.engine.connector.source;

import com.octopus.operators.engine.config.RuntimeEnvironment;
import com.octopus.operators.engine.config.source.SourceConfig;

public interface Source<R, T extends SourceConfig<?>, Env extends RuntimeEnvironment> {
  R read();

  void setConfig(T config);

  void setRuntimeEnvironment(Env runtimeEnvironment);
}
