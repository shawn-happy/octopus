package com.octopus.operators.spark;

import com.octopus.operators.engine.config.TaskConfig;
import com.octopus.operators.engine.connector.source.fake.FakeSourceConfig;
import com.octopus.operators.engine.connector.source.fake.FakeSourceConfig.FakeSourceOptions;
import com.octopus.operators.engine.connector.source.fake.FakeSourceConfig.FakeSourceRow;
import com.octopus.operators.spark.runtime.DatasetTableInfo;
import com.octopus.operators.spark.runtime.SparkJobContext;
import com.octopus.operators.spark.runtime.SparkRuntimeEnvironment;
import com.octopus.operators.spark.runtime.source.FakeSource;
import java.util.List;
import java.util.Map;
import org.junit.jupiter.api.Test;

public class FakeDemo {

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
            .build();
    SparkRuntimeEnvironment sparkRuntimeEnvironment = new SparkRuntimeEnvironment();
    sparkRuntimeEnvironment.setTaskConfig(config);
    sparkRuntimeEnvironment.prepare();

    SparkJobContext context = new SparkJobContext(sparkRuntimeEnvironment.getSparkSession());

    FakeSource fakeSource =
        new FakeSource(
            sparkRuntimeEnvironment, context, (FakeSourceConfig) config.getSources().get(0));
    DatasetTableInfo read = fakeSource.read();
    read.getDataset().show(1000);
  }
}
