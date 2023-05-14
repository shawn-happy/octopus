package com.octopus.kettlex.core.executor;

import com.octopus.kettlex.core.trans.TransMeta;

public interface TransExecutor {

  String getId();

  void setId(String instanceId);

  void execute(String[] args);

  void stop();

  boolean isStopped();

  void release();

  TransMeta getTransMeta();
}
