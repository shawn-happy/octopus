package com.octopus.spark.operators.runtime.step.transform.postProcess;

import com.octopus.spark.operators.declare.check.CheckLevel;
import com.octopus.spark.operators.declare.postprocess.CorrectionPostProcessDeclare;
import com.octopus.spark.operators.declare.postprocess.CorrectionPostProcessDeclare.CorrectionPostProcessOptions;
import com.octopus.spark.operators.declare.postprocess.CorrectionPostProcessMode;
import com.octopus.spark.operators.declare.postprocess.PostProcessDeclare.Alarm;
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
    Alarm alarm = declare.getAlarm();
    boolean enableAlarm = false;
    String hitCheck = null;
    if (alarm.getCheck() != null) {
      if (!checkResults.get(alarm.getCheck()).isPass()) {
        enableAlarm = true;
        hitCheck = alarm.getCheck();
      }
    }
    if (alarm.getLevel() != null) {
      CheckLevel checkLevel = getCheckLevel(checkResults);
      if (alarm.getLevel() == checkLevel) {
        enableAlarm = true;
      }
    }
    CorrectionPostProcessOptions options = declare.getOptions();
    CorrectionPostProcessMode mode = options.getMode();
    String source = options.getSource();
    if (enableAlarm) {
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
      if (hitCheck != null) {
        checkResults.get(hitCheck).setPostProcess(declare.getName());
      }
    }
  }

  private CheckLevel getCheckLevel(Map<String, CheckResult> checks) {
    CheckLevel checkLevel = null;
    for (Map.Entry<String, CheckResult> entry : checks.entrySet()) {
      if (!entry.getValue().isPass()) {
        if (checkLevel == null || checkLevel.getLevel() < entry.getValue().getLevel().getLevel()) {
          checkLevel = entry.getValue().getLevel();
        }
      }
    }
    return checkLevel;
  }

  private <V> String stringFormat(String template, Map<String, V> values) {
    StringSubstitutor sub = new StringSubstitutor(values);
    return sub.replace(template);
  }
}
