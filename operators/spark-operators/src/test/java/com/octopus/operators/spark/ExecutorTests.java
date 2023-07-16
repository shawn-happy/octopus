package com.octopus.operators.spark;

import com.octopus.operators.spark.registry.DefaultLoader;
import com.octopus.operators.spark.registry.Loader;
import com.octopus.operators.spark.registry.OpRegistry;
import com.octopus.operators.spark.runtime.executor.DataQualityExecutor;
import com.octopus.operators.spark.runtime.executor.ETLExecutor;
import com.octopus.operators.spark.runtime.executor.Executor;
import com.octopus.operators.spark.runtime.executor.ReportExecutor;
import org.apache.spark.sql.SparkSession;
import org.junit.jupiter.api.Disabled;
import org.junit.jupiter.api.Test;

@Disabled
public class ExecutorTests {

  @Test
  public void createETLExecutor() throws Exception {
    String path =
        DeclareTests.class.getClassLoader().getResource("etl-declare-example.yaml").getPath();
    SparkSession sparkSession =
        SparkSession.builder().appName("test").master("local[2]").getOrCreate();
    Executor executor = new ETLExecutor(sparkSession, path);
    executor.run();
  }

  @Test
  public void createReportExecutor() throws Exception {
    String path =
        DeclareTests.class.getClassLoader().getResource("report-declare-example.yaml").getPath();
    SparkSession sparkSession =
        SparkSession.builder().appName("test").master("local[2]").getOrCreate();
    Loader loader = new DefaultLoader(OpRegistry.OP_REGISTRY);
    loader.init();
    Executor executor = new ReportExecutor(sparkSession, path);
    executor.run();
  }

  @Test
  public void createDataQualityExecutor() throws Exception {
    String path =
        DeclareTests.class
            .getClassLoader()
            .getResource("data-quality-declare-example.yaml")
            .getPath();
    SparkSession sparkSession =
        SparkSession.builder().appName("test").master("local[2]").getOrCreate();
    Loader loader = new DefaultLoader(OpRegistry.OP_REGISTRY);
    loader.init();
    Executor executor = new DataQualityExecutor(sparkSession, path);
    executor.run();
  }

  @Test
  public void createETLExecutorWithJDBC() throws Exception {
    String path = DeclareTests.class.getClassLoader().getResource("etl-jdbc.yaml").getPath();
    SparkSession sparkSession =
        SparkSession.builder().appName("test").master("local[2]").getOrCreate();
    Loader loader = new DefaultLoader(OpRegistry.OP_REGISTRY);
    loader.init();
    Executor executor = new ETLExecutor(sparkSession, path);
    executor.run();
  }
}
