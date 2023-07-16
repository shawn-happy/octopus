package com.octopus.operators.spark.runtime.step.transform.metrics;

import com.octopus.operators.spark.declare.transform.TransformDeclare;
import com.octopus.operators.spark.declare.transform.TransformOptions;
import java.util.Map;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;

public abstract class BaseMetrics<MD extends TransformDeclare<? extends TransformOptions>>
    implements Metrics<MD> {

  protected final MD declare;

  protected BaseMetrics(MD declare) {
    this.declare = declare;
  }

  @Override
  public Object calculate(SparkSession spark, Map<String, Dataset<Row>> dfs) throws Exception {
    declare.verify();
    declare.getOptions().verify();
    return doMetrics(spark, dfs);
  }

  protected abstract Object doMetrics(SparkSession spark, Map<String, Dataset<Row>> dfs)
      throws Exception;
}
