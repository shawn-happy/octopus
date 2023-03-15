package com.shawn.octopus.spark.operators.common.declare.transform.postprocess;

import com.shawn.octopus.spark.operators.common.declare.transform.TransformDeclare;
import com.shawn.octopus.spark.operators.common.declare.transform.TransformOptions;
import com.shawn.octopus.spark.operators.common.declare.transform.check.CheckLevel;
import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Getter;
import lombok.NoArgsConstructor;

public interface PostProcessTransformDeclare<TO extends TransformOptions>
    extends TransformDeclare<TO> {

  PostProcessType getPostProcessType();

  Alarm getAlarm();

  @Builder
  @Getter
  @AllArgsConstructor
  @NoArgsConstructor
  class Alarm {
    String check;
    CheckLevel level;
  }
}
