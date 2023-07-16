package com.octopus.spark.operators.runtime.step.sink;

import com.octopus.spark.operators.declare.sink.SinkDeclare;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;

public interface Sink<SD extends SinkDeclare<?>> {

  void output(SparkSession spark, Dataset<Row> df) throws Exception;
}
