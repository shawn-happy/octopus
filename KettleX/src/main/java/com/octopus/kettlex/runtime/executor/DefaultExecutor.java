package com.octopus.kettlex.runtime.executor;

import com.fasterxml.jackson.core.type.TypeReference;
import com.octopus.kettlex.core.exception.KettleXJSONException;
import com.octopus.kettlex.core.utils.JsonUtil;
import com.octopus.kettlex.runtime.TaskCombination;

public class DefaultExecutor implements Executor {
  private final TaskCombination taskCombination;

  public DefaultExecutor(TaskCombination taskCombination) {
    this.taskCombination = taskCombination;
  }

  public DefaultExecutor(String configJson) {
    this.taskCombination =
        JsonUtil.fromJson(configJson, new TypeReference<TaskCombination>() {})
            .orElseThrow(() -> new KettleXJSONException("parse json error"));
  }

  @Override
  public void executor() {
    processReaders();
  }

  private void processReaders() {}
}
