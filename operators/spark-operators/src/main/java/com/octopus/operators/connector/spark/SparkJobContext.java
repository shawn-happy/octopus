package com.octopus.operators.connector.spark;

import com.octopus.operators.engine.connector.TaskContext;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import lombok.Getter;
import org.apache.commons.collections4.CollectionUtils;
import org.apache.commons.collections4.MapUtils;
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

  public void registerTable(String tableName, Dataset<Row> df) {
    df.createOrReplaceTempView(tableName);
    this.datasetMap.computeIfAbsent(tableName, e -> df);
  }

  public Dataset<Row> getDataset(String tableName) {
    return this.datasetMap.get(tableName);
  }

  public boolean tableExists(List<String> tables) {
    if (CollectionUtils.isEmpty(tables)) {
      return false;
    }
    if (MapUtils.isEmpty(datasetMap)) {
      return false;
    }
    for (String table : tables) {
      if (!datasetMap.containsKey(table)) {
        return false;
      }
    }
    return true;
  }
}
