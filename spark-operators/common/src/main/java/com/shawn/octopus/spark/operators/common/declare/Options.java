package com.shawn.octopus.spark.operators.common.declare;

import java.util.Map;

public interface Options {

  Map<String, String> getOptions();

  void verify();
}
