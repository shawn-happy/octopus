package com.shawn.octopus.spark.operators.data.quality.postProcess;

import com.shawn.octopus.spark.operators.common.declare.transform.postprocess.PostProcessTransformDeclare;
import com.shawn.octopus.spark.operators.data.quality.check.CheckResult;
import java.util.Map;
import org.apache.spark.sql.SparkSession;

public interface PostProcess<T extends PostProcessTransformDeclare<?>> {

  void process(SparkSession spark, Map<String, CheckResult> checkResults);

  T getDeclare();
}
