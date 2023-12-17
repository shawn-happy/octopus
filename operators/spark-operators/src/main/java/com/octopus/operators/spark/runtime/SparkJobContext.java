package com.octopus.operators.spark.runtime;

import java.util.HashMap;
import java.util.Map;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;

public class SparkJobContext {

  private SparkSession sparkSession;

  private final Map<String, Dataset<Row>> datasetMap = new HashMap<>();

  public Map<String, Dataset<Row>> getDatasetMap() {
    return datasetMap;
  }
}
