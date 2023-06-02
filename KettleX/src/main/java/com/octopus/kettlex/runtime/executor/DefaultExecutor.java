package com.octopus.kettlex.runtime.executor;

import com.fasterxml.jackson.core.type.TypeReference;
import com.octopus.kettlex.core.exception.KettleXJSONException;
import com.octopus.kettlex.core.utils.JsonUtil;
import com.octopus.kettlex.runtime.TaskGroup;

public class DefaultExecutor implements Executor {
  private final TaskGroup taskGroup;

  public DefaultExecutor(TaskGroup taskGroup) {
    this.taskGroup = taskGroup;
  }

  public DefaultExecutor(String configJson) {
    this.taskGroup =
        JsonUtil.fromJson(configJson, new TypeReference<TaskGroup>() {})
            .orElseThrow(() -> new KettleXJSONException("parse json error"));
  }

  @Override
  public void executor() {
    processReaders();
  }

  private void processReaders() {}
}
