package com.octopus.operators.spark.runtime;

import com.octopus.operators.engine.connector.TaskContext;
import java.util.HashMap;
import java.util.Map;
import lombok.Getter;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;

public class SparkJobContext extends TaskContext {

  @Getter private final SparkSession sparkSession;
  private final Map<String, Dataset<Row>> datasetMap;

  public SparkJobContext(SparkSession sparkSession) {
    super();
    this.sparkSession = sparkSession;
    this.datasetMap = new HashMap<>(2 << 4);
  }

  public void setDataset(String tableName, Dataset<Row> df) {
    this.datasetMap.computeIfAbsent(tableName, e -> df);
  }

  public Dataset<Row> getDataset(String tableName) {
    return this.datasetMap.get(tableName);
  }
}
