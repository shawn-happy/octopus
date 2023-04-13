package com.octopus.spark.operators.runtime;

import com.octopus.spark.operators.exception.SparkRuntimeException;
import com.octopus.spark.operators.runtime.executor.DataQualityExecutor;
import com.octopus.spark.operators.runtime.executor.ETLExecutor;
import com.octopus.spark.operators.runtime.executor.Executor;
import com.octopus.spark.operators.runtime.executor.ExecutorType;
import com.octopus.spark.operators.runtime.executor.ReportExecutor;
import com.octopus.spark.operators.utils.SparkOperatorUtils;
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
