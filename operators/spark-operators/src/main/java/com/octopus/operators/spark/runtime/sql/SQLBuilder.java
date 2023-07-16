package com.octopus.operators.spark.runtime.sql;

public interface SQLBuilder {

  SQLProvider getProvider();

  String insert();

  String insertOrUpdate();
}
