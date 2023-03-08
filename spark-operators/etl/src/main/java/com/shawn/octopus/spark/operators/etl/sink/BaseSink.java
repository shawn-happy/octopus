package com.shawn.octopus.spark.operators.etl.sink;

import com.shawn.octopus.spark.operators.common.declare.sink.SinkDeclare;
import com.shawn.octopus.spark.operators.common.declare.sink.SinkOptions;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;

public abstract class BaseSink<SD extends SinkDeclare<?>> implements Sink<SD> {

  @Override
  public void output(SparkSession spark, Dataset<Row> df) throws Exception {
    SD declare = getDeclare();
    SinkOptions sinkOptions = declare.getOptions();
    sinkOptions.verify();
    process(spark, df);
  }

  protected abstract void process(SparkSession spark, Dataset<Row> df) throws Exception;
}
