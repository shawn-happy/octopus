package com.shawn.octopus.spark.operators.data.quality.check;

import com.shawn.octopus.spark.operators.common.declare.transform.check.CheckTransformDeclare;
import java.util.Map;
import org.apache.spark.sql.SparkSession;

public interface Check<T extends CheckTransformDeclare> {

  boolean check(SparkSession spark, Map<String, Object> metrics);

  T getDeclare();
}
