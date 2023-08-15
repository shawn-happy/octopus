package com.octopus.operators.spark.source;

import com.google.common.io.Resources;
import com.octopus.operators.spark.declare.common.SinkType;
import com.octopus.operators.spark.declare.common.SourceType;
import com.octopus.operators.spark.declare.common.WriteMode;
import com.octopus.operators.spark.declare.sink.ConsoleSinkDeclare;
import com.octopus.operators.spark.declare.source.ParquetSourceDeclare;
import com.octopus.operators.spark.runtime.step.sink.ConsoleSink;
import com.octopus.operators.spark.runtime.step.source.ParquetSource;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import org.junit.jupiter.api.Test;

public class ParquetSourceTest {

  @Test
  public void parquetSource() throws Exception {
    SparkSession sparkSession =
        SparkSession.builder().appName("test").master("local[2]").getOrCreate();
    ParquetSourceDeclare declare =
        ParquetSourceDeclare.builder()
            .name("parquet")
            .type(SourceType.parquet)
            .output("output")
            .repartition(1)
            .options(
                ParquetSourceDeclare.ParquetSourceOptions.builder()
                    .paths(new String[] {Resources.getResource("user.parquet").getPath()})
                    .build())
            .build();
    ParquetSource parquetSource = new ParquetSource(declare);
    Dataset<Row> ds = parquetSource.input(sparkSession);

    ConsoleSinkDeclare consoleSinkDeclare =
        ConsoleSinkDeclare.builder()
            .name("console")
            .input("test")
            .type(SinkType.console)
            .writeMode(WriteMode.append)
            .options(ConsoleSinkDeclare.ConsoleSinkOptions.builder().build())
            .build();
    ConsoleSink consoleSink = new ConsoleSink(consoleSinkDeclare);
    consoleSink.output(sparkSession, ds);
  }
}
