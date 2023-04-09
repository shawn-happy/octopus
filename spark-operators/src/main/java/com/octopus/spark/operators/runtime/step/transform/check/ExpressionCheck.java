package com.octopus.spark.operators.runtime.step.transform.check;

import com.googlecode.aviator.AviatorEvaluator;
import com.octopus.spark.operators.declare.check.ExpressionCheckDeclare;
import java.util.Map;
import org.apache.spark.sql.SparkSession;

public class ExpressionCheck implements Check<ExpressionCheckDeclare> {

  private final ExpressionCheckDeclare declare;

  public ExpressionCheck(ExpressionCheckDeclare declare) {
    this.declare = declare;
  }

  @Override
  public boolean check(SparkSession spark, Map<String, Object> metrics) {
    return (boolean) AviatorEvaluator.execute(declare.getOptions().getExpression(), metrics);
  }
}
