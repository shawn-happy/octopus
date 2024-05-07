package com.octopus.operators.connector.flink;

import com.octopus.operators.connector.flink.sink.ConsoleSink;
import com.octopus.operators.connector.flink.source.FakeSource;
import com.octopus.operators.engine.config.TaskConfig;
import com.octopus.operators.engine.config.sink.SinkType;
import com.octopus.operators.engine.config.sink.WriteMode;
import com.octopus.operators.engine.connector.sink.console.ConsoleSinkConfig;
import com.octopus.operators.engine.connector.sink.console.ConsoleSinkConfig.ConsoleSinkOptions;
import com.octopus.operators.engine.connector.source.fake.FakeSourceConfig;
import com.octopus.operators.engine.connector.source.fake.FakeSourceConfig.FakeSourceOptions;
import com.octopus.operators.engine.connector.source.fake.FakeSourceConfig.FakeSourceRow;
import java.util.List;
import java.util.Map;
import org.junit.jupiter.api.Test;

public class ConsoleSinkTests {

  @Test
  public void test() {
    TaskConfig config =
        TaskConfig.builder()
            .runtimeConfig(Map.of("spark.master", "local[4]", "spark.driver.cores", 1))
            .taskName("fake-demo")
            .sources(
                List.of(
                    FakeSourceConfig.builder()
                        .name("fake-source")
                        .output("fake")
                        .parallelism(1)
                        .options(
                            FakeSourceOptions.builder()
                                .rowNum(100)
                                .fields(
                                    new FakeSourceRow[] {
                                      FakeSourceRow.builder()
                                          .fieldName("age")
                                          .fieldType("int")
                                          .intMin(0)
                                          .intMax(120)
                                          .build()
                                    })
                                .build())
                        .build()))
            .sinks(
                List.of(
                    ConsoleSinkConfig.builder()
                        .name("console")
                        .type(SinkType.CONSOLE)
                        .options(ConsoleSinkOptions.builder().build())
                        .writeMode(WriteMode.APPEND)
                        .input("fake")
                        .build()))
            .build();
    FlinkRuntimeEnvironment flinkRuntimeEnvironment = new FlinkRuntimeEnvironment();
    flinkRuntimeEnvironment.setTaskConfig(config);
    flinkRuntimeEnvironment.prepare();

    FlinkJobContext context = new FlinkJobContext(flinkRuntimeEnvironment);
    FakeSource fakeSource =
        new FakeSource(
            flinkRuntimeEnvironment, context, (FakeSourceConfig) config.getSources().get(0));
    DataStreamTableInfo read = fakeSource.read();
    ConsoleSink consoleSink =
        new ConsoleSink(
            flinkRuntimeEnvironment, context, (ConsoleSinkConfig) config.getSinks().get(0));
    consoleSink.output(read);
  }
}
