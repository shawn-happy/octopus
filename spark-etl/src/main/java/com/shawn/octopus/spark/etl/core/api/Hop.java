package com.shawn.octopus.spark.etl.core.api;

public interface Hop {

  default String getId() {
    return getFrom().getId() + "->" + getTo().getId();
  }

  Operation getFrom();

  Operation getTo();
}
