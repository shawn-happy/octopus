package com.shawn.octopus.spark.operators.report.metrics;

import com.shawn.octopus.spark.operators.common.declare.transform.metrics.CustomMetricsTransformDeclare;
import java.util.Map;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;

public class CustomMetrics implements Metrics<CustomMetricsTransformDeclare> {

  private final CustomMetricsTransformDeclare declare;
  private final String sql;

  public CustomMetrics(CustomMetricsTransformDeclare declare) {
    this.declare = declare;
    this.sql = declare.getOptions().getSql();
  }

  @Override
  public Dataset<Row> calculate(SparkSession spark, Map<String, Dataset<Row>> dfs)
      throws Exception {
    for (Map.Entry<String, Dataset<Row>> df : dfs.entrySet()) {
      df.getValue().createOrReplaceTempView(df.getKey());
    }
    return spark.sql(sql);
  }

  @Override
  public CustomMetricsTransformDeclare getDeclare() {
    return declare;
  }
}
