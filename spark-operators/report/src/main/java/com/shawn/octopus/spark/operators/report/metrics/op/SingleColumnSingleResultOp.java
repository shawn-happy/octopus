package com.shawn.octopus.spark.operators.report.metrics.op;

import com.shawn.octopus.spark.operators.report.metrics.converter.DatasetToObjectConverter;
import java.util.List;
import java.util.Map;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;

public enum SingleColumnSingleResultOp implements Op<Object> {
  MAX_OP {
    @Override
    public String sqlPattern() {
      return "SELECT max(%2$s) FROM %1$s";
    }
  },

  MIN_OP {
    @Override
    public String sqlPattern() {
      return "SELECT min(%2$s) FROM %1$s";
    }
  },
  ;

  private static final DatasetToObjectConverter converter =
      DatasetToObjectConverter.DATASET_TO_OBJECT_CONVERTER;

  @Override
  public Object process(SparkSession spark, Map<String, Dataset<Row>> dfs, List<String> columns)
      throws Exception {
    Map.Entry<String, Dataset<Row>> df = dfs.entrySet().iterator().next();
    df.getValue().createOrReplaceTempView(df.getKey());
    String sql = String.format(sqlPattern(), df.getKey(), columns.get(0));
    return converter.convert(spark.sql(sql));
  }

  public abstract String sqlPattern();
}
