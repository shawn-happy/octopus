package com.octopus.spark.operators.runtime.executor;

import com.octopus.spark.operators.declare.DataQualityDeclare;
import com.octopus.spark.operators.declare.check.CheckDeclare;
import com.octopus.spark.operators.declare.check.ExpressionCheckDeclare;
import com.octopus.spark.operators.declare.common.CheckType;
import com.octopus.spark.operators.declare.postprocess.AlarmPostProcessDeclare;
import com.octopus.spark.operators.declare.postprocess.CorrectionPostProcessDeclare;
import com.octopus.spark.operators.declare.postprocess.PostProcessDeclare;
import com.octopus.spark.operators.declare.postprocess.PostProcessType;
import com.octopus.spark.operators.declare.source.SourceDeclare;
import com.octopus.spark.operators.declare.transform.MetricsDeclare;
import com.octopus.spark.operators.exception.SparkRuntimeException;
import com.octopus.spark.operators.runtime.factory.SourceFactory;
import com.octopus.spark.operators.runtime.factory.TransformFactory;
import com.octopus.spark.operators.runtime.step.source.Source;
import com.octopus.spark.operators.runtime.step.transform.check.Check;
import com.octopus.spark.operators.runtime.step.transform.check.CheckResult;
import com.octopus.spark.operators.runtime.step.transform.check.ExpressionCheck;
import com.octopus.spark.operators.runtime.step.transform.metrics.Metrics;
import com.octopus.spark.operators.runtime.step.transform.postProcess.AlarmPostProcess;
import com.octopus.spark.operators.runtime.step.transform.postProcess.CorrectionPostProcess;
import com.octopus.spark.operators.runtime.step.transform.postProcess.PostProcess;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.Map;
import lombok.extern.slf4j.Slf4j;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;

@Slf4j
public class DataQualityExecutor extends BaseExecutor {

  private final transient Map<String, Dataset<Row>> dataframes = new HashMap<>();
  private final transient Map<String, String> sources = new HashMap<>();
  private final transient Map<String, Object> metrics = new HashMap<>();
  private final transient Map<String, CheckResult> checkResults = new HashMap<>();

  public DataQualityExecutor(SparkSession spark, String configPath) {
    super(spark, configPath);
  }

  @Override
  protected void processSources() throws Exception {
    DataQualityDeclare dataQualityDeclare = (DataQualityDeclare) declare;
    for (SourceDeclare<?> sourceDeclare : dataQualityDeclare.getSources()) {
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
    DataQualityDeclare dataQualityDeclare = (DataQualityDeclare) declare;
    for (MetricsDeclare<?> metricsDeclare : dataQualityDeclare.getMetrics()) {
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
    if (dataQualityDeclare.getChecks() != null) {
      for (CheckDeclare<?> checkDeclare : dataQualityDeclare.getChecks()) {
        CheckType checkType = checkDeclare.getType();
        Check<?> check = null;
        switch (checkType) {
          case expression:
            check = new ExpressionCheck((ExpressionCheckDeclare) checkDeclare);
            break;
          default:
            throw new SparkRuntimeException("unsupported check type: " + checkType);
        }
        Map<String, Object> inputMetrics = new LinkedHashMap<>();
        checkDeclare
            .getMetrics()
            .forEach(
                m -> {
                  if (!metrics.containsKey(m)) {
                    log.error("input metric [{}] not found.", m);
                    throw new RuntimeException("input metric not found.");
                  }
                  inputMetrics.put(m, metrics.get(m));
                });
        boolean result = check.check(spark, inputMetrics);
        checkResults.put(
            checkDeclare.getName(),
            CheckResult.builder().pass(result).level(checkDeclare.getCheckLevel()).build());
      }
    }

    if (dataQualityDeclare.getPostProcesses() != null) {
      for (PostProcessDeclare<?> postProcessDeclare : dataQualityDeclare.getPostProcesses()) {
        PostProcessType type = postProcessDeclare.getType();
        PostProcess<?> postProcess = null;
        switch (type) {
          case correction:
            postProcess =
                new CorrectionPostProcess(
                    (CorrectionPostProcessDeclare) postProcessDeclare, sources);
            break;
          case alarm:
            postProcess = new AlarmPostProcess((AlarmPostProcessDeclare) postProcessDeclare);
            break;
          default:
            throw new SparkRuntimeException("unsupported post process type: " + type);
        }
        postProcess.process(spark, checkResults);
      }
    }
  }

  @Override
  protected void processSinks() throws Exception {}
}
