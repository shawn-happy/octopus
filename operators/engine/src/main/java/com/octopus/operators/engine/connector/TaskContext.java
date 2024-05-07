package com.octopus.operators.engine.connector;

import com.octopus.operators.engine.config.TaskMode;
import com.octopus.operators.engine.table.catalog.Table;
import java.util.Map;
import java.util.Optional;
import java.util.UUID;
import java.util.concurrent.ConcurrentHashMap;
import lombok.Getter;
import lombok.Setter;

@Getter
public class TaskContext {

  private final String taskId;
  @Setter public String taskName;
  @Setter private TaskMode taskMode;

  private final Map<String, Table> tableCache = new ConcurrentHashMap<>(2 << 4);

  public TaskContext() {
    this.taskId = UUID.randomUUID().toString().replace("-", "");
  }

  public TaskContext(String taskId) {
    this.taskId = taskId;
  }

  public void addTable(String tableName, Table table) {
    tableCache.computeIfAbsent(tableName, t -> table);
  }

  public Optional<Table> getTable(String tableName) {
    return Optional.ofNullable(tableCache.get(tableName));
  }
}
