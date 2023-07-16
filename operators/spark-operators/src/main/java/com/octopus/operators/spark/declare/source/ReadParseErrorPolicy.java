package com.octopus.operators.spark.declare.source;

public enum ReadParseErrorPolicy {
  PERMISSIVE,
  DROPMALFORMED,
  FAILFAST,
  ;
}
