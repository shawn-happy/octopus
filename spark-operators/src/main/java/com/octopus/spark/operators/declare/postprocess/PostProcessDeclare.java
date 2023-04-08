package com.octopus.spark.operators.declare.postprocess;

import com.octopus.spark.operators.declare.check.CheckLevel;
import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Getter;
import lombok.NoArgsConstructor;

public interface PostProcessDeclare<P extends PostProcessOptions> {

  PostProcessType getPostProcessType();

  String getName();

  Alarm getAlarm();

  P getOptions();

  @Builder
  @Getter
  @AllArgsConstructor
  @NoArgsConstructor
  class Alarm {
    String check;
    CheckLevel level;
  }
}
