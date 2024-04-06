package io.github.shawn.octopus.fluxus.engine.connector.transform.expression.function;

import com.googlecode.aviator.runtime.function.AbstractFunction;
import com.googlecode.aviator.runtime.function.FunctionUtils;
import com.googlecode.aviator.runtime.type.AviatorObject;
import com.googlecode.aviator.runtime.type.AviatorString;
import java.util.Map;

public class StrConcatFunction extends AbstractFunction {

  @Override
  public String getName() {
    return "str.concat";
  }

  @Override
  public AviatorObject call(Map<String, Object> env, AviatorObject arg1, AviatorObject arg2) {
    String argStr1 = (arg1.getValue(env) == null) ? "" : FunctionUtils.getStringValue(arg1, env);
    String argStr2 = (arg2.getValue(env) == null) ? "" : FunctionUtils.getStringValue(arg2, env);
    return new AviatorString(argStr1 + argStr2);
  }
}
