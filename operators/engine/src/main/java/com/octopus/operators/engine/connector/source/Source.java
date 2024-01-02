package com.octopus.operators.engine.connector.source;

import com.octopus.operators.engine.config.RuntimeEnvironment;
import com.octopus.operators.engine.config.source.SourceConfig;

public interface Source<Env extends RuntimeEnvironment, T extends SourceConfig<?>, R> {
  R read();
}
