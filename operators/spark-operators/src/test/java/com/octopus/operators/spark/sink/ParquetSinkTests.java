package com.octopus.operators.spark.sink;

import com.octopus.operators.spark.declare.common.SinkType;
import com.octopus.operators.spark.declare.common.WriteMode;
import com.octopus.operators.spark.declare.sink.ParquetSinkDeclare;
import com.octopus.operators.spark.declare.sink.ParquetSinkDeclare.ParquetSinkOptions;
import com.octopus.operators.spark.runtime.step.sink.ParquetSink;
import org.apache.spark.sql.SparkSession;
import org.junit.jupiter.api.Test;

public class ParquetSinkTests extends BaseSinkTests {

  @Test
  public void parquetSink() throws Exception {
    ParquetSinkDeclare declare =
        ParquetSinkDeclare.builder()
            .name("parquet")
            .input("csv")
            .options(ParquetSinkOptions.builder().path("output/parquet").build())
            .writeMode(WriteMode.append)
            .type(SinkType.parquet)
            .build();

    ParquetSink sink = new ParquetSink(declare);
    sink.output(SparkSession.builder().getOrCreate(), ds);
  }
}
