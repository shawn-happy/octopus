package com.octopus.spark.operators.runtime.step.sink;

import com.octopus.spark.operators.declare.sink.SinkDeclare;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;

public abstract class BaseSink<SD extends SinkDeclare<?>> implements Sink<SD> {

  protected final SD declare;

  public BaseSink(SD declare) {
    this.declare = declare;
  }

  @Override
  public void output(SparkSession spark, Dataset<Row> df) throws Exception {
    declare.verify();
    declare.getOptions().verify();
    process(spark, df);
  }

  protected abstract void process(SparkSession spark, Dataset<Row> df) throws Exception;
}
