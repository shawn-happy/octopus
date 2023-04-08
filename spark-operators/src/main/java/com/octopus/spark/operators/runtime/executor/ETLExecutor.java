package com.octopus.spark.operators.runtime.executor;

import com.octopus.spark.operators.declare.sink.SinkDeclare;
import com.octopus.spark.operators.declare.source.SourceDeclare;
import com.octopus.spark.operators.runtime.factory.SourceFactory;
import com.octopus.spark.operators.runtime.step.source.Source;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;

public class ETLExecutor extends BaseExecutor {

  private transient Map<String, Dataset<Row>> dataframes = new HashMap<>();

  protected ETLExecutor(SparkSession spark, String configPath) {
    super(spark, configPath);
  }

  @Override
  protected void processSources(List<SourceDeclare<?>> sourceDeclares) throws Exception {
    for (SourceDeclare<?> sourceDeclare : sourceDeclares) {
      Source<?> source = SourceFactory.createETLSource(sourceDeclare);
      Dataset<Row> df = source.input(spark);
      if (sourceDeclare.getRepartition() != null) {
        df.repartition(sourceDeclare.getRepartition());
      }
      dataframes.put(sourceDeclare.getOutput(), df);
    }
  }

  @Override
  protected void processTransforms() throws Exception {}

  @Override
  protected void processSinks(List<SinkDeclare<?>> sinkDeclares) throws Exception {}
}
