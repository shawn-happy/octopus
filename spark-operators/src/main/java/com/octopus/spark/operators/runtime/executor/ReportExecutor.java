package com.octopus.spark.operators.runtime.executor;

import com.octopus.spark.operators.declare.ReportDeclare;
import com.octopus.spark.operators.declare.source.SourceDeclare;
import com.octopus.spark.operators.declare.transform.MetricsDeclare;
import com.octopus.spark.operators.runtime.factory.SourceFactory;
import com.octopus.spark.operators.runtime.factory.TransformFactory;
import com.octopus.spark.operators.runtime.step.source.Source;
import com.octopus.spark.operators.runtime.step.transform.metrics.Metrics;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.Map;
import lombok.extern.slf4j.Slf4j;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;

@Slf4j
public class ReportExecutor extends BaseExecutor {

  private final transient Map<String, Dataset<Row>> dataframes = new HashMap<>();
  private final transient Map<String, String> sources = new HashMap<>();
  private final transient Map<String, Object> metrics = new HashMap<>();

  public ReportExecutor(SparkSession spark, String configPath) {
    super(spark, configPath);
  }

  @Override
  protected void processSources() throws Exception {
    ReportDeclare reportDeclare = (ReportDeclare) declare;
    for (SourceDeclare<?> sourceDeclare : reportDeclare.getSources()) {
      Source<?> source = SourceFactory.createSource(sourceDeclare);
      Dataset<Row> df = source.input(spark);
      if (sourceDeclare.getRepartition() != null) {
        df.repartition(sourceDeclare.getRepartition());
      }
      dataframes.put(sourceDeclare.getOutput(), df);
    }
  }

  @Override
  protected void processTransforms() throws Exception {
    ReportDeclare reportDeclare = (ReportDeclare) declare;
    for (MetricsDeclare<?> metricsDeclare : reportDeclare.getMetrics()) {
      Metrics<?> metricsOp = TransformFactory.createMetrics(metricsDeclare, this.metrics);
      Map<String, Dataset<Row>> dfs = new LinkedHashMap<>();
      metricsDeclare
          .getInput()
          .forEach(
              (k, v) -> {
                Dataset<Row> df = dataframes.get(k);
                if (df == null) {
                  log.error("input dataframe [{}] not found.", k);
                  throw new RuntimeException("input dataframe not found.");
                }
                dfs.put(v, df);
              });
      Object result = metricsOp.calculate(spark, dfs);
      metrics.put(metricsDeclare.getName(), result);
    }
  }

  @Override
  protected void processSinks() throws Exception {}
}
