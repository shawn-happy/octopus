package com.shawn.octopus.spark.operators.common.declare.sink;

import com.shawn.octopus.spark.operators.common.WriteMode;
import com.shawn.octopus.spark.operators.common.declare.Options;

public interface SinkOptions extends Options {

  WriteMode getWriteMode();

  String getInput();
}
