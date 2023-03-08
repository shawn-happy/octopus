package com.shawn.octopus.spark.operators.etl.transform;

import com.shawn.octopus.spark.operators.common.declare.transform.TransformDeclare;
import com.shawn.octopus.spark.operators.common.declare.transform.TransformOptions;
import java.util.Map;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;

public abstract class BaseTransform<TD extends TransformDeclare<?>> implements Transform<TD> {

  @Override
  public Dataset<Row> trans(SparkSession spark, Map<String, Dataset<Row>> dfs) throws Exception {
    TransformOptions transformOptions = getDeclare().getOptions();
    transformOptions.verify();
    return process(spark, dfs);
  }

  protected abstract Dataset<Row> process(SparkSession spark, Map<String, Dataset<Row>> dfs)
      throws Exception;
}
