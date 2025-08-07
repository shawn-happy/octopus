package com.octopus.operators.engine.runtime;

import com.octopus.operators.engine.exception.EngineException;
import com.octopus.operators.engine.exception.TableAlreadyExistsException;
import com.octopus.operators.engine.exception.TableNotFoundException;
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

  public void addResultSet(String resultTable, T resultSet) {
    if (tableMap.containsKey(resultTable)) {
      throw new TableAlreadyExistsException(resultTable);
    }
    tableMap.put(resultTable, resultSet);
  }

  public T getResultSet(String name) {
    return Optional.ofNullable(tableMap.get(name))
        .orElseThrow(() -> new TableNotFoundException(name));
  }
}
