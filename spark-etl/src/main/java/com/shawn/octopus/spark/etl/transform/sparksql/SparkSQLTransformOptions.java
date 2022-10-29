package com.shawn.octopus.spark.etl.transform.sparksql;

import com.shawn.octopus.spark.etl.transform.TransformOptions;
import java.util.HashMap;
import java.util.Map;
import java.util.Objects;
import org.apache.commons.lang3.StringUtils;

public class SparkSQLTransformOptions implements TransformOptions {

  private String output;
  private String sql;
  private Integer repartition;

  private SparkSQLTransformOptions() {}

  public String getSql() {
    return sql;
  }

  @Override
  public Integer getRePartition() {
    return repartition;
  }

  @Override
  public String output() {
    return output;
  }

  @Override
  public Map<String, String> getOptions() {
    Map<String, String> map = new HashMap<>();
    map.put("sql", sql);
    map.put("repartition", String.valueOf(repartition));
    return map;
  }

  public static SparkSQLTransformOptionsBuilder builder() {
    return new SparkSQLTransformOptionsBuilder();
  }

  public static class SparkSQLTransformOptionsBuilder {
    private String output;
    private String sql;
    private Integer repartition;

    public SparkSQLTransformOptionsBuilder output(String output) {
      this.output = output;
      return this;
    }

    public SparkSQLTransformOptionsBuilder sql(String sql) {
      this.sql = sql;
      return this;
    }

    public SparkSQLTransformOptionsBuilder repartition(Integer repartition) {
      this.repartition = repartition;
      return this;
    }

    public SparkSQLTransformOptions build() {
      valid();
      SparkSQLTransformOptions options = new SparkSQLTransformOptions();
      options.output = this.output;
      options.sql = this.sql;
      options.repartition = this.repartition;
      return options;
    }

    private void valid() {
      if (StringUtils.isEmpty(output)) {
        throw new IllegalArgumentException("transform must have output data");
      }
      if (StringUtils.isBlank(sql)) {
        throw new IllegalArgumentException("transform must have sql");
      }
      if (Objects.nonNull(repartition) && repartition < 0) {
        throw new IllegalArgumentException("repartition can not be less than 0");
      }
    }
  }
}
