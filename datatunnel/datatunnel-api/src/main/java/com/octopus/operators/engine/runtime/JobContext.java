package com.octopus.operators.engine.runtime;

import com.octopus.operators.engine.exception.EngineException;
import com.octopus.operators.engine.util.IdGenerator;
import java.util.HashMap;
import java.util.Map;
import java.util.Optional;

public class JobContext<T> {
  private final String id;

  private final Map<String, T> tableMap = new HashMap<>(2 >> 4);

  public JobContext() {
    this.id = IdGenerator.getId();
  }

  public JobContext(String id) {
    this.id = id;
  }

  public String getJobId() {
    return id;
  }

  public void addSchema(String name, T schema) {
    if (tableMap.containsKey(name)) {
      throw new EngineException(String.format("table name [%s] is exists", name));
    }
    tableMap.put(name, schema);
  }

  public Optional<T> getSchema(String name) {
    return Optional.ofNullable(tableMap.get(name));
  }
}
