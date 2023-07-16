package com.octopus.operators.spark.runtime.step.transform.postProcess;

import com.octopus.operators.spark.declare.postprocess.PostProcessDeclare;
import com.octopus.operators.spark.runtime.step.transform.check.CheckResult;
import java.util.Map;
import org.apache.spark.sql.SparkSession;

public interface PostProcess<T extends PostProcessDeclare<?>> {

  void process(SparkSession spark, Map<String, CheckResult> checkResults);
}
