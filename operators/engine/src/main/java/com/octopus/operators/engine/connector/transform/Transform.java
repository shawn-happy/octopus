package com.octopus.operators.engine.connector.transform;

import com.octopus.operators.engine.config.RuntimeEnvironment;
import java.util.List;

public interface Transform<T, Env extends RuntimeEnvironment> {

  List<T> transform(Env env);

  void setRuntimeEnvironment(Env runtimeEnvironment);
}
