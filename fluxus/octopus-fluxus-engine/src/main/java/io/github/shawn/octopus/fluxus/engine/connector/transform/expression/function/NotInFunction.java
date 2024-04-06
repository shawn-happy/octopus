package io.github.shawn.octopus.fluxus.engine.connector.transform.expression.function;

import com.googlecode.aviator.runtime.function.AbstractFunction;
import com.googlecode.aviator.runtime.type.AviatorBoolean;
import com.googlecode.aviator.runtime.type.AviatorObject;
import java.util.Map;

public class NotInFunction extends AbstractFunction {

  @Override
  public String getName() {
    return "object.not_in";
  }

  private AviatorBoolean callpre(Map<String, Object> env, AviatorObject... args) {
    Object obj1 = args[0].getValue(env);
    for (int i = 1; i < args.length; i++) {
      Object tempObj = args[i].getValue(env);
      if (obj1.equals(tempObj)) {
        return AviatorBoolean.valueOf(false);
      }
    }
    return AviatorBoolean.valueOf(true);
  }

  @Override
  public AviatorObject call(Map<String, Object> env, AviatorObject arg1, AviatorObject arg2) {
    return callpre(env, arg1, arg2);
  }

  @Override
  public AviatorObject call(
      Map<String, Object> env, AviatorObject arg1, AviatorObject arg2, AviatorObject arg3) {
    return callpre(env, arg1, arg2, arg3);
  }

  @Override
  public AviatorObject call(
      Map<String, Object> env,
      AviatorObject arg1,
      AviatorObject arg2,
      AviatorObject arg3,
      AviatorObject arg4) {
    return callpre(env, arg1, arg2, arg3, arg4);
  }

  @Override
  public AviatorObject call(
      Map<String, Object> env,
      AviatorObject arg1,
      AviatorObject arg2,
      AviatorObject arg3,
      AviatorObject arg4,
      AviatorObject arg5) {
    return callpre(env, arg1, arg2, arg3, arg4, arg5);
  }

  @Override
  public AviatorObject call(
      Map<String, Object> env,
      AviatorObject arg1,
      AviatorObject arg2,
      AviatorObject arg3,
      AviatorObject arg4,
      AviatorObject arg5,
      AviatorObject arg6) {
    return callpre(env, arg1, arg2, arg3, arg4, arg5, arg6);
  }

  @Override
  public AviatorObject call(
      Map<String, Object> env,
      AviatorObject arg1,
      AviatorObject arg2,
      AviatorObject arg3,
      AviatorObject arg4,
      AviatorObject arg5,
      AviatorObject arg6,
      AviatorObject arg7) {
    return callpre(env, arg1, arg2, arg3, arg4, arg5, arg6, arg7);
  }

  @Override
  public AviatorObject call(
      Map<String, Object> env,
      AviatorObject arg1,
      AviatorObject arg2,
      AviatorObject arg3,
      AviatorObject arg4,
      AviatorObject arg5,
      AviatorObject arg6,
      AviatorObject arg7,
      AviatorObject arg8) {
    return callpre(env, arg1, arg2, arg3, arg4, arg5, arg6, arg7, arg8);
  }

  @Override
  public AviatorObject call(
      Map<String, Object> env,
      AviatorObject arg1,
      AviatorObject arg2,
      AviatorObject arg3,
      AviatorObject arg4,
      AviatorObject arg5,
      AviatorObject arg6,
      AviatorObject arg7,
      AviatorObject arg8,
      AviatorObject arg9) {
    return callpre(env, arg1, arg2, arg3, arg4, arg5, arg6, arg7, arg8, arg9);
  }

  @Override
  public AviatorObject call(
      Map<String, Object> env,
      AviatorObject arg1,
      AviatorObject arg2,
      AviatorObject arg3,
      AviatorObject arg4,
      AviatorObject arg5,
      AviatorObject arg6,
      AviatorObject arg7,
      AviatorObject arg8,
      AviatorObject arg9,
      AviatorObject arg10) {
    return callpre(env, arg1, arg2, arg3, arg4, arg5, arg6, arg7, arg8, arg9, arg10);
  }

  @Override
  public AviatorObject call(
      Map<String, Object> env,
      AviatorObject arg1,
      AviatorObject arg2,
      AviatorObject arg3,
      AviatorObject arg4,
      AviatorObject arg5,
      AviatorObject arg6,
      AviatorObject arg7,
      AviatorObject arg8,
      AviatorObject arg9,
      AviatorObject arg10,
      AviatorObject arg11) {
    return callpre(env, arg1, arg2, arg3, arg4, arg5, arg6, arg7, arg8, arg9, arg10, arg11);
  }

  @Override
  public AviatorObject call(
      Map<String, Object> env,
      AviatorObject arg1,
      AviatorObject arg2,
      AviatorObject arg3,
      AviatorObject arg4,
      AviatorObject arg5,
      AviatorObject arg6,
      AviatorObject arg7,
      AviatorObject arg8,
      AviatorObject arg9,
      AviatorObject arg10,
      AviatorObject arg11,
      AviatorObject arg12) {
    return callpre(env, arg1, arg2, arg3, arg4, arg5, arg6, arg7, arg8, arg9, arg10, arg11, arg12);
  }

  @Override
  public AviatorObject call(
      Map<String, Object> env,
      AviatorObject arg1,
      AviatorObject arg2,
      AviatorObject arg3,
      AviatorObject arg4,
      AviatorObject arg5,
      AviatorObject arg6,
      AviatorObject arg7,
      AviatorObject arg8,
      AviatorObject arg9,
      AviatorObject arg10,
      AviatorObject arg11,
      AviatorObject arg12,
      AviatorObject arg13) {
    return callpre(
        env, arg1, arg2, arg3, arg4, arg5, arg6, arg7, arg8, arg9, arg10, arg11, arg12, arg13);
  }

  @Override
  public AviatorObject call(
      Map<String, Object> env,
      AviatorObject arg1,
      AviatorObject arg2,
      AviatorObject arg3,
      AviatorObject arg4,
      AviatorObject arg5,
      AviatorObject arg6,
      AviatorObject arg7,
      AviatorObject arg8,
      AviatorObject arg9,
      AviatorObject arg10,
      AviatorObject arg11,
      AviatorObject arg12,
      AviatorObject arg13,
      AviatorObject arg14) {
    return callpre(
        env, arg1, arg2, arg3, arg4, arg5, arg6, arg7, arg8, arg9, arg10, arg11, arg12, arg13,
        arg14);
  }

  @Override
  public AviatorObject call(
      Map<String, Object> env,
      AviatorObject arg1,
      AviatorObject arg2,
      AviatorObject arg3,
      AviatorObject arg4,
      AviatorObject arg5,
      AviatorObject arg6,
      AviatorObject arg7,
      AviatorObject arg8,
      AviatorObject arg9,
      AviatorObject arg10,
      AviatorObject arg11,
      AviatorObject arg12,
      AviatorObject arg13,
      AviatorObject arg14,
      AviatorObject arg15) {
    return callpre(
        env, arg1, arg2, arg3, arg4, arg5, arg6, arg7, arg8, arg9, arg10, arg11, arg12, arg13,
        arg14, arg15);
  }

  @Override
  public AviatorObject call(
      Map<String, Object> env,
      AviatorObject arg1,
      AviatorObject arg2,
      AviatorObject arg3,
      AviatorObject arg4,
      AviatorObject arg5,
      AviatorObject arg6,
      AviatorObject arg7,
      AviatorObject arg8,
      AviatorObject arg9,
      AviatorObject arg10,
      AviatorObject arg11,
      AviatorObject arg12,
      AviatorObject arg13,
      AviatorObject arg14,
      AviatorObject arg15,
      AviatorObject arg16) {
    return callpre(
        env, arg1, arg2, arg3, arg4, arg5, arg6, arg7, arg8, arg9, arg10, arg11, arg12, arg13,
        arg14, arg15, arg16);
  }

  @Override
  public AviatorObject call(
      Map<String, Object> env,
      AviatorObject arg1,
      AviatorObject arg2,
      AviatorObject arg3,
      AviatorObject arg4,
      AviatorObject arg5,
      AviatorObject arg6,
      AviatorObject arg7,
      AviatorObject arg8,
      AviatorObject arg9,
      AviatorObject arg10,
      AviatorObject arg11,
      AviatorObject arg12,
      AviatorObject arg13,
      AviatorObject arg14,
      AviatorObject arg15,
      AviatorObject arg16,
      AviatorObject arg17) {
    return callpre(
        env, arg1, arg2, arg3, arg4, arg5, arg6, arg7, arg8, arg9, arg10, arg11, arg12, arg13,
        arg14, arg15, arg16, arg17);
  }

  @Override
  public AviatorObject call(
      Map<String, Object> env,
      AviatorObject arg1,
      AviatorObject arg2,
      AviatorObject arg3,
      AviatorObject arg4,
      AviatorObject arg5,
      AviatorObject arg6,
      AviatorObject arg7,
      AviatorObject arg8,
      AviatorObject arg9,
      AviatorObject arg10,
      AviatorObject arg11,
      AviatorObject arg12,
      AviatorObject arg13,
      AviatorObject arg14,
      AviatorObject arg15,
      AviatorObject arg16,
      AviatorObject arg17,
      AviatorObject arg18) {
    return callpre(
        env, arg1, arg2, arg3, arg4, arg5, arg6, arg7, arg8, arg9, arg10, arg11, arg12, arg13,
        arg14, arg15, arg16, arg17, arg18);
  }

  @Override
  public AviatorObject call(
      Map<String, Object> env,
      AviatorObject arg1,
      AviatorObject arg2,
      AviatorObject arg3,
      AviatorObject arg4,
      AviatorObject arg5,
      AviatorObject arg6,
      AviatorObject arg7,
      AviatorObject arg8,
      AviatorObject arg9,
      AviatorObject arg10,
      AviatorObject arg11,
      AviatorObject arg12,
      AviatorObject arg13,
      AviatorObject arg14,
      AviatorObject arg15,
      AviatorObject arg16,
      AviatorObject arg17,
      AviatorObject arg18,
      AviatorObject arg19) {
    return callpre(
        env, arg1, arg2, arg3, arg4, arg5, arg6, arg7, arg8, arg9, arg10, arg11, arg12, arg13,
        arg14, arg15, arg16, arg17, arg18, arg19);
  }

  @Override
  public AviatorObject call(
      Map<String, Object> env,
      AviatorObject arg1,
      AviatorObject arg2,
      AviatorObject arg3,
      AviatorObject arg4,
      AviatorObject arg5,
      AviatorObject arg6,
      AviatorObject arg7,
      AviatorObject arg8,
      AviatorObject arg9,
      AviatorObject arg10,
      AviatorObject arg11,
      AviatorObject arg12,
      AviatorObject arg13,
      AviatorObject arg14,
      AviatorObject arg15,
      AviatorObject arg16,
      AviatorObject arg17,
      AviatorObject arg18,
      AviatorObject arg19,
      AviatorObject arg20) {
    return callpre(
        env, arg1, arg2, arg3, arg4, arg5, arg6, arg7, arg8, arg9, arg10, arg11, arg12, arg13,
        arg14, arg15, arg16, arg17, arg18, arg19, arg20);
  }

  @Override
  public AviatorObject call(
      Map<String, Object> env,
      AviatorObject arg1,
      AviatorObject arg2,
      AviatorObject arg3,
      AviatorObject arg4,
      AviatorObject arg5,
      AviatorObject arg6,
      AviatorObject arg7,
      AviatorObject arg8,
      AviatorObject arg9,
      AviatorObject arg10,
      AviatorObject arg11,
      AviatorObject arg12,
      AviatorObject arg13,
      AviatorObject arg14,
      AviatorObject arg15,
      AviatorObject arg16,
      AviatorObject arg17,
      AviatorObject arg18,
      AviatorObject arg19,
      AviatorObject arg20,
      AviatorObject... args) {
    AviatorBoolean rsq1 =
        callpre(
            env, arg1, arg2, arg3, arg4, arg5, arg6, arg7, arg8, arg9, arg10, arg11, arg12, arg13,
            arg14, arg15, arg16, arg17, arg18, arg19, arg20);
    AviatorBoolean rsq2 = callpre(env, args);
    return AviatorBoolean.valueOf(rsq1.booleanValue(env) && rsq2.booleanValue(env));
  }
}
