package com.octopus.operators.engine.connector.transform;

import com.octopus.operators.engine.config.RuntimeEnvironment;

public interface Transform<T, Env extends RuntimeEnvironment> {

  T transform();
}
