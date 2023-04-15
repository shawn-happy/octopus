package com.octopus.spark.operators.runtime.executor;

import com.octopus.spark.operators.declare.CommonDeclare;
import com.octopus.spark.operators.declare.source.SourceDeclare;
import com.octopus.spark.operators.runtime.factory.SourceFactory;
import com.octopus.spark.operators.runtime.step.source.Source;
import com.octopus.spark.operators.utils.SparkOperatorUtils;
import java.util.HashMap;
import java.util.Map;
import lombok.extern.slf4j.Slf4j;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;

@Slf4j
public abstract class BaseExecutor<T extends CommonDeclare> implements Executor {

  protected final SparkSession spark;
  protected final T declare;
  private final transient Map<String, Dataset<Row>> dataframes = new HashMap<>();

  protected BaseExecutor(SparkSession spark, String configPath, Class<T> tClass) {
    this.spark = spark;
    this.declare = getConfig(configPath, tClass);
  }

  @Override
  public void run() throws Exception {
    processSources();
    processTransforms();
    processSinks();
  }

  protected void processSources() throws Exception {
    for (SourceDeclare<?> sourceDeclare : declare.getSources()) {
      Source<?> source = SourceFactory.createSource(sourceDeclare);
      Dataset<Row> df = source.input(spark);
      if (sourceDeclare.getRepartition() != null) {
        df.repartition(sourceDeclare.getRepartition());
      }
      df.createOrReplaceTempView(sourceDeclare.getOutput());
      dataframes.put(sourceDeclare.getOutput(), df);
    }
  }

  protected abstract void processTransforms() throws Exception;

  protected abstract void processSinks() throws Exception;

  private T getConfig(String configPath, Class<T> tClass) {
    T declare = SparkOperatorUtils.getConfig(configPath, tClass);
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

  protected Map<String, Dataset<Row>> getDataframes() {
    return dataframes;
  }
}
