package com.shawn.octopus.spark.operators.etl;

import org.junit.jupiter.api.Test;

public class ExecutorTests {

  @Test
  public void testCSVExecutor() throws Exception {
    String path =
        Thread.currentThread().getContextClassLoader().getResource("csv_executor.yaml").getPath();
    Executor executor = new Executor();
    executor.run(path, true, false);
  }
}
