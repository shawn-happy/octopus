package com.octopus.operators.spark.runtime.step.transform.check;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.googlecode.aviator.AviatorEvaluator;
import com.octopus.operators.spark.declare.check.ExpressionCheckDeclare;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import org.apache.spark.sql.SparkSession;

public class ExpressionCheck implements Check<ExpressionCheckDeclare> {

  private final ExpressionCheckDeclare declare;
  private final ObjectMapper objectMapper = new ObjectMapper();

  public ExpressionCheck(ExpressionCheckDeclare declare) {
    this.declare = declare;
  }

  @Override
  public boolean check(SparkSession spark, Map<String, Object> metrics) throws Exception {
    for (Entry<String, Object> entry : metrics.entrySet()) {
      Object value = entry.getValue();
      if (value instanceof List<?>) {
        metrics.put(entry.getKey(), ((List<?>) value).get(0));
      }
    }
    return (boolean) AviatorEvaluator.execute(declare.getOptions().getExpression(), metrics);
  }
}
