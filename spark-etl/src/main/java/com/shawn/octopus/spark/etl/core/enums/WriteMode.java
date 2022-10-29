package com.shawn.octopus.spark.etl.core.enums;

public enum WriteMode {
  append,
  replace,
  create_or_replace,
  replace_by_time,
  overwrite_partition,
  ;
}
