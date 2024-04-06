package io.github.shawn.octopus.data.fluxus.engine.connector.transform.expression;

import com.googlecode.aviator.AviatorEvaluator;
import com.googlecode.aviator.Expression;
import io.github.shawn.octopus.data.fluxus.engine.connector.transform.expression.function.DateFormatFunction;
import java.util.HashMap;
import java.util.Map;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

class ExpressionFunctionTest {

  @Test
  void dateFormatFunctionTest() {
    AviatorEvaluator.addFunction(new DateFormatFunction());
    String expression = "date.format(e1,e2)";
    Expression compile = AviatorEvaluator.compile(expression);
    Map<String, Object> env = new HashMap<>();
    env.put("e1", "yyyy-MM-dd");
    env.put("e2", "2024-02-15 14:55:50");
    Object execute = compile.execute(env);
    Assertions.assertNotNull(execute);
  }
}
