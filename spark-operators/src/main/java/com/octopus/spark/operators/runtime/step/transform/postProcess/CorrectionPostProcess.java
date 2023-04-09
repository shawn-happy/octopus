package com.octopus.spark.operators.runtime.step.transform.postProcess;

import com.octopus.spark.operators.declare.postprocess.CorrectionPostProcessDeclare;
import com.octopus.spark.operators.declare.postprocess.CorrectionPostProcessDeclare.CorrectionPostProcessOptions;
import com.octopus.spark.operators.declare.postprocess.CorrectionPostProcessMode;
import com.octopus.spark.operators.runtime.step.transform.check.CheckResult;
import java.util.Map;
import lombok.extern.slf4j.Slf4j;
import org.apache.commons.text.StringSubstitutor;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;

@Slf4j
public class CorrectionPostProcess implements PostProcess<CorrectionPostProcessDeclare> {

  private static final String CORRECTED_TABLE_SUFFIX = "_corrected";
  private final CorrectionPostProcessDeclare declare;
  private final Map<String, String> sources;

  public CorrectionPostProcess(CorrectionPostProcessDeclare declare, Map<String, String> sources) {
    this.declare = declare;
    this.sources = sources;
  }

  @Override
  public void process(SparkSession spark, Map<String, CheckResult> checkResults) {

    CorrectionPostProcessOptions options = declare.getOptions();
    CorrectionPostProcessMode mode = options.getMode();
    String source = options.getSource();
    switch (mode) {
      case modify:
        String sql = stringFormat(options.getSql(), sources);
        spark.sql(sql);
        break;
      case replace:
        Dataset<Row> df = spark.read().table(sources.get(source));
        df.createOrReplaceTempView(source);
        df.writeTo(source + CORRECTED_TABLE_SUFFIX).createOrReplace();
        break;
      default:
        throw new IllegalArgumentException("unsupported post-process correction mode: " + mode);
    }
  }

  private <V> String stringFormat(String template, Map<String, V> values) {
    StringSubstitutor sub = new StringSubstitutor(values);
    return sub.replace(template);
  }
}
