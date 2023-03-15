package com.shawn.octopus.spark.operators.report.metrics.op;

import com.shawn.octopus.spark.operators.common.declare.transform.metrics.BuiltinMetricsOpType;
import com.shawn.octopus.spark.operators.report.metrics.converter.DatasetToMapConverter;
import java.util.List;
import java.util.Map;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;

public enum MapObjectOp implements Op<Map<Object, Object>> {
  DISTRIBUTION_OP {
    @Override
    public BuiltinMetricsOpType getOpType() {
      return BuiltinMetricsOpType.distribution;
    }

    @Override
    public Map<Object, Object> process(
        SparkSession spark, Map<String, Dataset<Row>> dfs, List<String> columns) throws Exception {
      Map.Entry<String, Dataset<Row>> df = dfs.entrySet().iterator().next();
      df.getValue().createOrReplaceTempView(df.getKey());
      String sql =
          String.format(
              "SELECT %2$s,count(*) FROM %1$s GROUP BY %2$s", df.getKey(), columns.get(0));
      return DatasetToMapConverter.DATASET_TO_MULTI_ROW_MAP_OBJECT_CONVERTER.convert(
          spark.sql(sql));
    }
  }
}
