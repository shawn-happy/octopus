package com.octopus.operators.spark.runtime.step.transform.custom;

import com.octopus.operators.spark.declare.transform.TransformDeclare;
import java.util.Map;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;

public abstract class BaseTransform<TD extends TransformDeclare<?>> implements CustomTransform<TD> {

  protected final TD declare;

  public BaseTransform(TD declare) {
    this.declare = declare;
  }

  @Override
  public Dataset<Row> trans(SparkSession spark, Map<String, Dataset<Row>> dfs) throws Exception {
    declare.verify();
    declare.getOptions().verify();
    return process(spark, dfs);
  }

  protected abstract Dataset<Row> process(SparkSession spark, Map<String, Dataset<Row>> dfs)
      throws Exception;
}
