package com.octopus.operators.flink.declare.common;

public enum WriteMode {
  append,
  replace,
  create_or_replace,
  replace_by_time,
  overwrite_partitions,
  ;
}
