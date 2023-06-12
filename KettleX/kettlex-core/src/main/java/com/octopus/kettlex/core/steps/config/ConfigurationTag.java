package com.octopus.kettlex.core.steps.config;

public interface ConfigurationTag {

  interface TaskConfigurationTag {
    String TASK_ID = "taskId";
    String TASK_NAME = "taskName";
    String TASK_VERSION = "version";
    String TASK_DESCRIPTION = "description";

    String TASK_READER_CONFIGS = "readers";
    String TASK_TRANSFORMATION_CONFIGS = "transforms";
    String TASK_WRITER_CONFIGS = "writers";

    String TASK_RUNTIME_CONFIG = "runtimeConfig";
  }

  interface TaskRuntimeConfigTag {
    String CHANNEL_CAPACITY = "channelCapcacity";
    String PARAMS = "params";
  }
}
