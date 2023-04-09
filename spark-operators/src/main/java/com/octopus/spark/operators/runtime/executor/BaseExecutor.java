package com.octopus.spark.operators.runtime.executor;

import com.octopus.spark.operators.declare.CommonDeclare;
import com.octopus.spark.operators.utils.SparkOperatorUtils;
import java.util.Map;
import lombok.extern.slf4j.Slf4j;
import org.apache.spark.sql.SparkSession;

@Slf4j
public abstract class BaseExecutor implements Executor {

  protected final SparkSession spark;
  protected final CommonDeclare declare;

  protected BaseExecutor(SparkSession spark, String configPath) {
    this.spark = spark;
    this.declare = getConfig(configPath);
  }

  @Override
  public void run() throws Exception {
    processSources();
    processTransforms();
    processSinks();
  }

  protected abstract void processSources() throws Exception;

  protected abstract void processTransforms() throws Exception;

  protected abstract void processSinks() throws Exception;

  private CommonDeclare getConfig(String configPath) {
    CommonDeclare declare = SparkOperatorUtils.getConfig(configPath, CommonDeclare.class);
    if (declare.getSparkConf() != null) {
      for (Map.Entry<String, String> kv : declare.getSparkConf().entrySet()) {
        if (spark.conf().isModifiable(kv.getKey())) {
          spark.conf().set(kv.getKey(), kv.getValue());
        } else {
          log.warn("spark conf [{}] can not set runtime.", kv.getKey());
        }
      }
    }

    if (declare.getParams() != null) {
      for (Map.Entry<String, String> kv : declare.getParams().entrySet()) {
        spark.sql("set " + kv.getKey() + "=" + kv.getValue());
      }
    }
    return declare;
  }
}
