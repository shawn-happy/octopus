package com.shawn.octopus.spark.etl.core;

public enum ReadParseErrorPolicy {
  PERMISSIVE,
  DROPMALFORMED,
  FAILFAST,
  ;
}
