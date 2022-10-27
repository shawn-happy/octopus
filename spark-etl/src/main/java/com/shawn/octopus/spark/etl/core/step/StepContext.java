package com.shawn.octopus.spark.etl.core.step;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;

public class StepContext {

  private final SparkSession session;
  private final List<String> dfNames;
  private final Map<String, Dataset<Row>> dfs;

  public StepContext(SparkSession session) {
    this.session = session;
    this.dfNames = new ArrayList<>();
    this.dfs = new HashMap<>();
  }

  public SparkSession getSparkSession() {
    return session;
  }

  public Dataset<Row> getDataFrame(String df) {
    return dfs.get(df);
  }

  public void setDataFrame(String name, Dataset<Row> df) {
    dfs.put(name, df);
    if (!dfNames.contains(name)) {
      dfNames.add(name);
    }
  }
}
