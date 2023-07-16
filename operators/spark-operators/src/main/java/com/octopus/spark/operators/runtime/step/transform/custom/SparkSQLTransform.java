package com.octopus.spark.operators.runtime.step.transform.custom;

import com.octopus.spark.operators.declare.transform.SparkSQLTransformDeclare;
import com.octopus.spark.operators.declare.transform.SparkSQLTransformDeclare.SparkSQLTransformOptions;
import java.util.Map;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;

public class SparkSQLTransform extends BaseTransform<SparkSQLTransformDeclare> {

  public SparkSQLTransform(SparkSQLTransformDeclare declare) {
    super(declare);
  }

  @Override
  protected Dataset<Row> process(SparkSession spark, Map<String, Dataset<Row>> dfs)
      throws Exception {
    for (Map.Entry<String, Dataset<Row>> df : dfs.entrySet()) {
      df.getValue().createOrReplaceTempView(df.getKey());
    }
    SparkSQLTransformOptions transformOptions = declare.getOptions();
    return spark.sql(transformOptions.getSql());
  }
}
