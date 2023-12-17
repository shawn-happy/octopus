package com.octopus.operators.spark.runtime;

import lombok.AllArgsConstructor;
import lombok.Getter;
import lombok.NoArgsConstructor;
import lombok.Setter;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;

@Getter
@Setter
@NoArgsConstructor
@AllArgsConstructor
public class DatasetTableInfo {

  private Dataset<Row> dataset;
  private String tableName;

  public static DatasetTableInfo of(Dataset<Row> ds, String tableName) {
    return new DatasetTableInfo(ds, tableName);
  }
}
