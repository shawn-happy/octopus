package com.octopus.operators.spark.runtime;

import com.octopus.operators.engine.config.CheckResult;
import com.octopus.operators.engine.config.EngineType;
import com.octopus.operators.engine.config.RuntimeEnvironment;
import com.octopus.operators.engine.config.TaskConfig;
import lombok.Getter;
import lombok.Setter;
import org.apache.commons.collections4.MapUtils;
import org.apache.commons.lang3.StringUtils;
import org.apache.spark.SparkConf;
import org.apache.spark.sql.SparkSession;

public class SparkRuntimeEnvironment implements RuntimeEnvironment {

  @Getter private final EngineType engine = EngineType.SPARK;
  @Setter @Getter private TaskConfig taskConfig;
  @Getter private SparkSession sparkSession;
  @Getter private SparkConf sparkConf;
  private String taskName = "spark-job";
  @Setter @Getter private boolean enableHive;

  @Override
  public CheckResult checkTaskConfig() {
    return null;
  }

  @Override
  public RuntimeEnvironment prepare() {
    String taskName = taskConfig.getTaskName();
    if (StringUtils.isNotBlank(taskName)) {
      this.taskName = taskName;
    }
    this.sparkConf = createSparkConf();
    this.sparkSession = createSparkSession();
    return null;
  }

  private SparkConf createSparkConf() {
    SparkConf sparkConf = new SparkConf();
    this.taskConfig
        .getRuntimeConfig()
        .forEach((key, value) -> sparkConf.set(key, String.valueOf(value)));
    sparkConf.setAppName(taskName);
    return sparkConf;
  }

  private SparkSession createSparkSession() {
    SparkSession.Builder builder = SparkSession.builder().config(sparkConf);
    if (enableHive) {
      builder.enableHiveSupport();
    }
    SparkSession sparkSession = builder.getOrCreate();
    if (MapUtils.isNotEmpty(taskConfig.getExtraParams())) {
      taskConfig
          .getExtraParams()
          .forEach((key, value) -> this.sparkSession.sql("set " + key + "=" + value));
    }
    return sparkSession;
  }
}
