package com.octopus.operators.spark;

import com.google.common.io.Resources;
import com.octopus.operators.spark.declare.common.SinkType;
import com.octopus.operators.spark.declare.common.WriteMode;
import com.octopus.operators.spark.declare.sink.ConsoleSinkDeclare;
import com.octopus.operators.spark.runtime.step.sink.ConsoleSink;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import org.junit.jupiter.api.Test;

public class ConsoleSinkTest {

  @Test
  public void consoleSink() throws Exception {
    ConsoleSinkDeclare declare =
        ConsoleSinkDeclare.builder()
            .name("console")
            .input("test")
            .type(SinkType.console)
            .writeMode(WriteMode.append)
            .options(ConsoleSinkDeclare.ConsoleSinkOptions.builder().build())
            .build();
    ConsoleSink consoleSink = new ConsoleSink(declare);
    SparkSession sparkSession =
        SparkSession.builder().appName("test").master("local[2]").getOrCreate();
    Dataset<Row> ds = sparkSession.read().csv(Resources.getResource("user.csv").getPath());
    consoleSink.output(SparkSession.builder().getOrCreate(), ds);
  }
}
