package com.octopus.operators.spark.sink;

import com.octopus.operators.spark.declare.common.SinkType;
import com.octopus.operators.spark.declare.common.WriteMode;
import com.octopus.operators.spark.declare.sink.ConsoleSinkDeclare;
import com.octopus.operators.spark.runtime.step.sink.ConsoleSink;
import org.apache.spark.sql.SparkSession;
import org.junit.jupiter.api.Test;

public class ConsoleSinkTest extends BaseSinkTests {

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
    consoleSink.output(SparkSession.builder().getOrCreate(), ds);
  }
}
