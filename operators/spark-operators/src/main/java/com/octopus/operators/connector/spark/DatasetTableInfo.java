package com.octopus.operators.connector.spark;

import lombok.Getter;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;

@Getter
public class DatasetTableInfo {

  private final Dataset<Row> dataset;
  private final String tableName;

  private DatasetTableInfo(String tableName, Dataset<Row> dataset) {
    this.tableName = tableName;
    this.dataset = dataset;
  }

  public static DatasetTableInfo of(String tableName, Dataset<Row> ds) {
    return new DatasetTableInfo(tableName, ds);
  }
}
