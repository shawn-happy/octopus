package com.octopus.operators.connector.flink;

import com.octopus.operators.connector.flink.source.FakeSource;
import com.octopus.operators.engine.config.TaskConfig;
import com.octopus.operators.engine.connector.source.fake.FakeSourceConfig;
import com.octopus.operators.engine.connector.source.fake.FakeSourceConfig.FakeSourceOptions;
import com.octopus.operators.engine.connector.source.fake.FakeSourceConfig.FakeSourceRow;
import java.util.List;
import java.util.Map;
import org.junit.jupiter.api.Test;

public class FakeSourceTests {

  @Test
  public void test() throws Exception {
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
            .build();
    FlinkRuntimeEnvironment runtimeEnvironment = new FlinkRuntimeEnvironment();
    runtimeEnvironment.setTaskConfig(config);
    runtimeEnvironment.prepare();

    FlinkJobContext jobContext = new FlinkJobContext(runtimeEnvironment);
    FakeSource fakeSource =
        new FakeSource(
            runtimeEnvironment, jobContext, (FakeSourceConfig) config.getSources().get(0));
    DataStreamTableInfo tableInfo = fakeSource.read();
    tableInfo.getDataStream().print();
    jobContext.getExecutionEnvironment().execute(config.getTaskName());
  }
}
