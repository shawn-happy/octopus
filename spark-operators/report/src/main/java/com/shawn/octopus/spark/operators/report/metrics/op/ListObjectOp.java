package com.shawn.octopus.spark.operators.report.metrics.op;

import com.shawn.octopus.spark.operators.common.declare.transform.metrics.BuiltinMetricsOpType;
import com.shawn.octopus.spark.operators.report.metrics.converter.DatasetToListObjectConverter;
import java.util.List;
import java.util.Map;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;

public enum ListObjectOp implements Op<List<Object>> {
  DISTINCT_OP {
    @Override
    public BuiltinMetricsOpType getOpType() {
      return BuiltinMetricsOpType.distinct;
    }

    @Override
    public List<Object> process(
        SparkSession spark, Map<String, Dataset<Row>> dfs, List<String> columns) throws Exception {
      Map.Entry<String, Dataset<Row>> df = dfs.entrySet().iterator().next();
      df.getValue().createOrReplaceTempView(df.getKey());
      String sql = String.format("SELECT distinct %2$s FROM %1$s", df.getKey(), columns.get(0));
      return DatasetToListObjectConverter.DATASET_TO_LIST_OBJECT_CONVERTER.convert(spark.sql(sql));
    }
  }
}
