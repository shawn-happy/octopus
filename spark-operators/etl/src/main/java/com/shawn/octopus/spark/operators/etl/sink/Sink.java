package com.shawn.octopus.spark.operators.etl.sink;

import com.shawn.octopus.spark.operators.common.declare.sink.SinkDeclare;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SaveMode;
import org.apache.spark.sql.SparkSession;

public interface Sink<SD extends SinkDeclare<?>> {

  SD getDeclare();

  SaveMode getSaveMode();

  void output(SparkSession spark, Dataset<Row> df) throws Exception;
}
