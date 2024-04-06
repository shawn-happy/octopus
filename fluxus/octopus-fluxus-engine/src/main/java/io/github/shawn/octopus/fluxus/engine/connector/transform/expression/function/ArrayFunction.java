package io.github.shawn.octopus.fluxus.engine.connector.transform.expression.function;

import com.googlecode.aviator.runtime.function.AbstractVariadicFunction;
import com.googlecode.aviator.runtime.type.AviatorObject;
import com.googlecode.aviator.runtime.type.AviatorRuntimeJavaType;
import java.util.Map;
import org.apache.commons.lang3.ArrayUtils;

public class ArrayFunction extends AbstractVariadicFunction {
  @Override
  public String getName() {
    return "str.array";
  }

  @Override
  public AviatorObject variadicCall(Map<String, Object> env, AviatorObject... args) {
    if (ArrayUtils.isEmpty(args)) {
      return null;
    }
    String[] values = new String[args.length];
    for (int i = 0; i < args.length; i++) {
      values[i] = String.valueOf(args[i].getValue(env));
    }
    return new AviatorRuntimeJavaType(values);
  }
}
