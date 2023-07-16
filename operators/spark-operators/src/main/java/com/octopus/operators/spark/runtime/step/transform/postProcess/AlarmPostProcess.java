package com.octopus.operators.spark.runtime.step.transform.postProcess;

import com.octopus.operators.spark.declare.postprocess.AlarmPostProcessDeclare;
import com.octopus.operators.spark.runtime.step.transform.check.CheckResult;
import java.util.Map;
import org.apache.spark.sql.SparkSession;

public class AlarmPostProcess implements PostProcess<AlarmPostProcessDeclare> {

  private final AlarmPostProcessDeclare declare;

  public AlarmPostProcess(AlarmPostProcessDeclare declare) {
    this.declare = declare;
  }

  @Override
  public void process(SparkSession spark, Map<String, CheckResult> checkResults) {}
}
