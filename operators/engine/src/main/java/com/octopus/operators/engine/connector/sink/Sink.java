package com.octopus.operators.engine.connector.sink;

import com.octopus.operators.engine.config.RuntimeEnvironment;

public interface Sink<T, Env extends RuntimeEnvironment> {

  void output(T t, Env env);

  void setRuntimeEnvironment(Env runtimeEnvironment);
}
