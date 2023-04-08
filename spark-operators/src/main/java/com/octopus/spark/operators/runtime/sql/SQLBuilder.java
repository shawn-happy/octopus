package com.octopus.spark.operators.runtime.sql;

public interface SQLBuilder {

  SQLProvider getProvider();

  String insert();

  String insertOrUpdate();
}
