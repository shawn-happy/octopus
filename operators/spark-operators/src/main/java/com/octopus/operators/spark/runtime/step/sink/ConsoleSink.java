package com.octopus.operators.spark.runtime.step.sink;

import com.octopus.operators.spark.declare.sink.ConsoleSinkDeclare;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;

public class ConsoleSink extends BaseSink<ConsoleSinkDeclare> {

  public ConsoleSink(ConsoleSinkDeclare declare) {
    super(declare);
  }

  @Override
  protected void process(SparkSession spark, Dataset<Row> df) throws Exception {
    df.show();
  }
}
