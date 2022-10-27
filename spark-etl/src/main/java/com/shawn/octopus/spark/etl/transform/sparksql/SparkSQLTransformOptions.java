package com.shawn.octopus.spark.etl.transform.sparksql;

import com.shawn.octopus.spark.etl.core.common.TableDesc;
import com.shawn.octopus.spark.etl.transform.TransformOptions;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import org.apache.commons.collections.CollectionUtils;
import org.apache.commons.lang3.StringUtils;

public class SparkSQLTransformOptions implements TransformOptions {

  private List<TableDesc> outputs;
  private List<TableDesc> inputs;
  private String sql;
  private int repartition;

  private SparkSQLTransformOptions() {}

  public String getSql() {
    return sql;
  }

  @Override
  public int getRePartitions() {
    return repartition;
  }

  @Override
  public List<TableDesc> getInputs() {
    return inputs;
  }

  @Override
  public List<TableDesc> getOutputs() {
    return inputs;
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
    private List<TableDesc> outputs;
    private List<TableDesc> inputs;
    private String sql;
    private int repartition;

    public SparkSQLTransformOptionsBuilder outputs(List<TableDesc> outputs) {
      this.outputs = outputs;
      return this;
    }

    public SparkSQLTransformOptionsBuilder inputs(List<TableDesc> inputs) {
      this.inputs = inputs;
      return this;
    }

    public SparkSQLTransformOptionsBuilder sql(String sql) {
      this.sql = sql;
      return this;
    }

    public SparkSQLTransformOptionsBuilder repartition(int repartition) {
      this.repartition = repartition;
      return this;
    }

    public SparkSQLTransformOptions build() {
      valid();
      SparkSQLTransformOptions options = new SparkSQLTransformOptions();
      options.inputs = this.inputs;
      options.outputs = this.outputs;
      options.sql = this.sql;
      options.repartition = this.repartition;
      return options;
    }

    private void valid() {
      if (CollectionUtils.isEmpty(inputs)) {
        throw new IllegalArgumentException("transform must have input data");
      }
      if (CollectionUtils.isEmpty(outputs)) {
        throw new IllegalArgumentException("transform must have output data");
      }
      if (StringUtils.isBlank(sql)) {
        throw new IllegalArgumentException("transform must have sql");
      }
      if (repartition < 0) {
        throw new IllegalArgumentException("repartition can not be less than 0");
      }
    }
  }
}
