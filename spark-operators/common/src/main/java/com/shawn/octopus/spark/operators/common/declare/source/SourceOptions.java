package com.shawn.octopus.spark.operators.common.declare.source;

import com.shawn.octopus.spark.operators.common.declare.Options;

public interface SourceOptions extends Options {
  String getOutput();

  Integer getRepartition();
}
