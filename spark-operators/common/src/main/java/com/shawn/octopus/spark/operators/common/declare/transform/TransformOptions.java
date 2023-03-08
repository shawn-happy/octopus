package com.shawn.octopus.spark.operators.common.declare.transform;

import com.shawn.octopus.spark.operators.common.declare.Options;
import java.util.Map;

public interface TransformOptions extends Options {

  Integer getRepartition();

  String getOutput();

  Map<String, String> getInput();
}
