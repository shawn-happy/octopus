package com.shawn.octopus.spark.operators.data.quality.check;

import com.googlecode.aviator.AviatorEvaluator;
import com.shawn.octopus.spark.operators.common.declare.transform.check.CheckTransformDeclare;
import java.util.Map;
import org.apache.spark.sql.SparkSession;

public class ExpressionCheck implements Check<CheckTransformDeclare> {

  private final CheckTransformDeclare declare;

  public ExpressionCheck(CheckTransformDeclare declare) {
    this.declare = declare;
  }

  @Override
  public boolean check(SparkSession spark, Map<String, Object> metrics) {
    return (boolean) AviatorEvaluator.execute(getDeclare().getOptions().getExpression(), metrics);
  }

  @Override
  public CheckTransformDeclare getDeclare() {
    return declare;
  }
}
