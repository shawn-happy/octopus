package com.shawn.octopus.spark.operators.common;

public enum WriteMode {
  append,
  replace,
  create_or_replace,
  replace_by_time,
  overwrite_partition,
  ;
}
