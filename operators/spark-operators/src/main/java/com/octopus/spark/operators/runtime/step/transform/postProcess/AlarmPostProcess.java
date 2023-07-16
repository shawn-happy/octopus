package com.octopus.spark.operators.runtime.step.transform.postProcess;

import com.octopus.spark.operators.declare.postprocess.AlarmPostProcessDeclare;
import com.octopus.spark.operators.runtime.step.transform.check.CheckResult;
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
