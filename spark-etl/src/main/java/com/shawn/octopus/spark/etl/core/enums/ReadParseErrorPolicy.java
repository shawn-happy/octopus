package com.shawn.octopus.spark.etl.core.enums;

public enum ReadParseErrorPolicy {
  PERMISSIVE,
  DROPMALFORMED,
  FAILFAST,
  ;
}
