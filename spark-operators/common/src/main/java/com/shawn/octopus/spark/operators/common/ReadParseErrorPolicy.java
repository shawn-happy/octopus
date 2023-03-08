package com.shawn.octopus.spark.operators.common;

public enum ReadParseErrorPolicy {
  PERMISSIVE,
  DROPMALFORMED,
  FAILFAST,
  ;
}
