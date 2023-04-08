package com.octopus.spark.operators.runtime.executor;

import com.octopus.spark.operators.declare.sink.SinkDeclare;
import com.octopus.spark.operators.declare.source.SourceDeclare;
import java.util.List;
import org.apache.spark.sql.SparkSession;

public class DataQualityExecutor extends BaseExecutor {

  protected DataQualityExecutor(SparkSession spark, String configPath) {
    super(spark, configPath);
  }

  @Override
  protected void processSources(List<SourceDeclare<?>> sourceDeclares) throws Exception {}

  @Override
  protected void processTransforms() throws Exception {}

  @Override
  protected void processSinks(List<SinkDeclare<?>> sinkDeclares) throws Exception {}
}
