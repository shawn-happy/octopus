package com.octopus.spark.operators.declare.common;

import java.util.Map;

public interface Options {

  Map<String, String> getOptions();

  void verify();
}
