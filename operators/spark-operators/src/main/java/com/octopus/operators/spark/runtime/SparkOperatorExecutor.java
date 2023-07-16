package com.octopus.operators.spark.runtime;

import com.octopus.operators.spark.exception.SparkRuntimeException;
import com.octopus.operators.spark.registry.DefaultLoader;
import com.octopus.operators.spark.registry.Loader;
import com.octopus.operators.spark.registry.OpRegistry;
import com.octopus.operators.spark.runtime.executor.DataQualityExecutor;
import com.octopus.operators.spark.runtime.executor.ETLExecutor;
import com.octopus.operators.spark.runtime.executor.Executor;
import com.octopus.operators.spark.runtime.executor.ExecutorType;
import com.octopus.operators.spark.runtime.executor.ReportExecutor;
import com.octopus.operators.spark.utils.SparkOperatorUtils;
import java.nio.charset.StandardCharsets;
import java.util.Base64;
import lombok.extern.slf4j.Slf4j;
import org.apache.spark.sql.SparkSession;

@Slf4j
public class SparkOperatorExecutor {

  public static void main(String[] args) throws Exception {
    String configBase64 = args[0];
    log.info("spark runtime instance base64: {}", configBase64);
    SparkRuntimeConfig config =
        SparkOperatorUtils.getConfig(
            Base64.getDecoder().decode(configBase64.getBytes(StandardCharsets.UTF_8)),
            SparkRuntimeConfig.class);

    String type = config.getExecutorType();
    ExecutorType executorType = ExecutorType.of(type);
    String configPath = config.getConfigPath();
    Loader loader = new DefaultLoader(OpRegistry.OP_REGISTRY);
    loader.init();
    try (SparkSession spark = SparkOperatorUtils.createSparkSession(config)) {
      Executor executor = null;
      switch (executorType) {
        case ETL:
          executor = new ETLExecutor(spark, configPath);
          break;
        case REPORT:
          executor = new ReportExecutor(spark, configPath);
          break;
        case DATA_QUALITY:
          executor = new DataQualityExecutor(spark, configPath);
          break;
        default:
          throw new SparkRuntimeException("unsupported executor type: " + type);
      }
      executor.run();
    }
  }
}
