package com.octopus.operators.spark.sink;

import com.google.common.io.Resources;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import org.junit.jupiter.api.BeforeEach;

public class BaseSinkTests {
  protected SparkSession sparkSession;
  protected Dataset<Row> ds;

  @BeforeEach
  public void init() {
    sparkSession = SparkSession.builder().appName("test").master("local[2]").getOrCreate();
    ds = sparkSession.read().csv(Resources.getResource("user.csv").getPath());
  }
}
