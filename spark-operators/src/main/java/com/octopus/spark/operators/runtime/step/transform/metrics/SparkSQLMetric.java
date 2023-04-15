package com.octopus.spark.operators.runtime.step.transform.metrics;

import com.octopus.spark.operators.declare.transform.SparkSQLTransformDeclare;
import com.octopus.spark.operators.runtime.step.transform.metrics.converter.DatasetToListObjectConverter;
import java.util.Map;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;

public class SparkSQLMetric extends BaseMetrics<SparkSQLTransformDeclare> {

  public SparkSQLMetric(SparkSQLTransformDeclare declare) {
    super(declare);
  }

  @Override
  protected Object doMetrics(SparkSession spark, Map<String, Dataset<Row>> dfs) throws Exception {
    for (Map.Entry<String, Dataset<Row>> df : dfs.entrySet()) {
      df.getValue().createOrReplaceTempView(df.getKey());
    }
    return DatasetToListObjectConverter.DATASET_TO_JSON_ARRAY_CONVERTER.convert(
        spark.sql(declare.getOptions().getSql()));
  }
}
