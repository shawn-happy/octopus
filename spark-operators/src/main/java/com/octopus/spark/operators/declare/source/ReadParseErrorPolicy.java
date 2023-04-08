package com.octopus.spark.operators.declare.source;

public enum ReadParseErrorPolicy {
  PERMISSIVE,
  DROPMALFORMED,
  FAILFAST,
  ;
}
