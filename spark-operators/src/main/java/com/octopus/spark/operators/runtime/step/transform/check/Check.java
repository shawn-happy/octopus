package com.octopus.spark.operators.runtime.step.transform.check;

import com.octopus.spark.operators.declare.check.CheckDeclare;
import com.octopus.spark.operators.declare.check.CheckOptions;
import java.util.Map;
import org.apache.spark.sql.SparkSession;

public interface Check<T extends CheckDeclare<? extends CheckOptions>> {

  boolean check(SparkSession spark, Map<String, Object> metrics) throws Exception;
}
