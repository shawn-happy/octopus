package com.shawn.octopus.spark.operators.report;

import org.junit.jupiter.api.Test;

public class ExecutorTests {

  @Test
  public void test() throws Exception {
    String path =
        Thread.currentThread()
            .getContextClassLoader()
            .getResource("csv_builtin_metrics.yaml")
            .getPath();
    Executor executor = new Executor();
    executor.run(path);
  }
}
