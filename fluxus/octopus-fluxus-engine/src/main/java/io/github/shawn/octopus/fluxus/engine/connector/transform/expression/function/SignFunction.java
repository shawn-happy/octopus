package io.github.shawn.octopus.fluxus.engine.connector.transform.expression.function;

import com.googlecode.aviator.runtime.function.AbstractFunction;
import com.googlecode.aviator.runtime.function.FunctionUtils;
import com.googlecode.aviator.runtime.type.AviatorObject;
import com.googlecode.aviator.runtime.type.AviatorString;
import io.github.shawn.octopus.fluxus.api.exception.DataWorkflowException;
import io.github.shawn.octopus.fluxus.engine.connector.transform.expression.SignUtil;
import java.util.HashMap;
import java.util.Map;
import org.apache.commons.lang3.StringUtils;

public class SignFunction extends AbstractFunction {

  private static final String ERR_MSG = "sign.format Aviator arg1 is blank";

  @Override
  public String getName() {
    return "sign.format";
  }

  /**
   * 创建默认的sign
   *
   * @param env Variable environment
   */
  @Override
  public AviatorObject call(Map<String, Object> env) {
    return new AviatorString(
        SignUtil.getInstance()
            .createSign(
                new HashMap<>(),
                SignUtil.APP_PASSWORD,
                System.currentTimeMillis() + "",
                SignUtil.HASH_TYPE_SHA256));
  }

  /** 创建带密码的sign */
  @Override
  public AviatorObject call(Map<String, Object> env, AviatorObject arg1) {
    String argStr1 = (arg1.getValue(env) == null) ? "" : FunctionUtils.getStringValue(arg1, env);
    if (StringUtils.isBlank(argStr1)) {
      throw new DataWorkflowException(ERR_MSG);
    }
    return new AviatorString(
        SignUtil.getInstance()
            .createSign(
                new HashMap<>(),
                argStr1,
                System.currentTimeMillis() + "",
                SignUtil.HASH_TYPE_SHA256));
  }

  /** 创建带密码和时间戳的sign */
  @Override
  public AviatorObject call(Map<String, Object> env, AviatorObject arg1, AviatorObject arg2) {
    String argStr1 = (arg1.getValue(env) == null) ? "" : FunctionUtils.getStringValue(arg1, env);
    Long argStr2 =
        (arg2.getValue(env) == null) ? 0L : (Long) FunctionUtils.getNumberValue(arg2, env);
    if (StringUtils.isBlank(argStr1)) {
      throw new DataWorkflowException(ERR_MSG);
    }
    return new AviatorString(
        SignUtil.getInstance()
            .createSign(
                new HashMap<>(), argStr1, String.valueOf(argStr2), SignUtil.HASH_TYPE_SHA256));
  }

  /** 创建带密码和时间戳，加密方式的sign */
  @Override
  public AviatorObject call(
      Map<String, Object> env, AviatorObject arg1, AviatorObject arg2, AviatorObject arg3) {
    String argStr1 = (arg1.getValue(env) == null) ? "" : FunctionUtils.getStringValue(arg1, env);
    Long argStr2 =
        (arg2.getValue(env) == null) ? 0L : (Long) FunctionUtils.getNumberValue(arg2, env);
    String argStr3 = (arg3.getValue(env) == null) ? "" : FunctionUtils.getStringValue(arg3, env);
    if (StringUtils.isBlank(argStr1)) {
      throw new DataWorkflowException(ERR_MSG);
    }
    Map<String, Object> requestMap = new HashMap<>();
    return new AviatorString(
        SignUtil.getInstance().createSign(requestMap, argStr1, String.valueOf(argStr2), argStr3));
  }

  @Override
  public AviatorObject call(
      Map<String, Object> env,
      AviatorObject arg1,
      AviatorObject arg2,
      AviatorObject arg3,
      AviatorObject arg4) {
    String argStr1 = (arg1.getValue(env) == null) ? "" : FunctionUtils.getStringValue(arg1, env);
    Long argStr2 =
        (arg2.getValue(env) == null) ? 0L : (Long) FunctionUtils.getNumberValue(arg2, env);
    String argStr3 = (arg3.getValue(env) == null) ? "" : FunctionUtils.getStringValue(arg3, env);
    if (StringUtils.isBlank(argStr1)) {
      throw new DataWorkflowException(ERR_MSG);
    }
    Map<String, Object> requestMap = new HashMap<>();
    return new AviatorString(
        SignUtil.getInstance().createSign(requestMap, argStr1, String.valueOf(argStr2), argStr3));
  }

  @Override
  public AviatorObject call(
      Map<String, Object> env,
      AviatorObject arg1,
      AviatorObject arg2,
      AviatorObject arg3,
      AviatorObject arg4,
      AviatorObject arg5) {
    String argStr1 = (arg1.getValue(env) == null) ? "" : FunctionUtils.getStringValue(arg1, env);
    Long argStr2 =
        (arg2.getValue(env) == null) ? 0L : (Long) FunctionUtils.getNumberValue(arg2, env);
    String argStr3 = (arg3.getValue(env) == null) ? "" : FunctionUtils.getStringValue(arg3, env);
    if (StringUtils.isBlank(argStr1)) {
      throw new DataWorkflowException(ERR_MSG);
    }
    Map<String, Object> requestMap = new HashMap<>();
    return new AviatorString(
        SignUtil.getInstance().createSign(requestMap, argStr1, String.valueOf(argStr2), argStr3));
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
    String argStr1 = (arg1.getValue(env) == null) ? "" : FunctionUtils.getStringValue(arg1, env);
    Long argStr2 =
        (arg2.getValue(env) == null) ? 0L : (Long) FunctionUtils.getNumberValue(arg2, env);
    String argStr3 = (arg3.getValue(env) == null) ? "" : FunctionUtils.getStringValue(arg3, env);
    if (StringUtils.isBlank(argStr1)) {
      throw new DataWorkflowException(ERR_MSG);
    }
    Map<String, Object> requestMap = new HashMap<>();
    return new AviatorString(
        SignUtil.getInstance().createSign(requestMap, argStr1, String.valueOf(argStr2), argStr3));
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
    String argStr1 = (arg1.getValue(env) == null) ? "" : FunctionUtils.getStringValue(arg1, env);
    Long argStr2 =
        (arg2.getValue(env) == null) ? 0L : (Long) FunctionUtils.getNumberValue(arg2, env);
    String argStr3 = (arg3.getValue(env) == null) ? "" : FunctionUtils.getStringValue(arg3, env);
    if (StringUtils.isBlank(argStr1)) {
      throw new DataWorkflowException(ERR_MSG);
    }
    Map<String, Object> requestMap = new HashMap<>();
    return new AviatorString(
        SignUtil.getInstance().createSign(requestMap, argStr1, String.valueOf(argStr2), argStr3));
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
    String argStr1 = (arg1.getValue(env) == null) ? "" : FunctionUtils.getStringValue(arg1, env);
    Long argStr2 =
        (arg2.getValue(env) == null) ? 0L : (Long) FunctionUtils.getNumberValue(arg2, env);
    String argStr3 = (arg3.getValue(env) == null) ? "" : FunctionUtils.getStringValue(arg3, env);
    if (StringUtils.isBlank(argStr1)) {
      throw new DataWorkflowException(ERR_MSG);
    }
    Map<String, Object> requestMap = new HashMap<>();
    return new AviatorString(
        SignUtil.getInstance().createSign(requestMap, argStr1, String.valueOf(argStr2), argStr3));
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
    String argStr1 = (arg1.getValue(env) == null) ? "" : FunctionUtils.getStringValue(arg1, env);
    Long argStr2 =
        (arg2.getValue(env) == null) ? 0L : (Long) FunctionUtils.getNumberValue(arg2, env);
    String argStr3 = (arg3.getValue(env) == null) ? "" : FunctionUtils.getStringValue(arg3, env);
    if (StringUtils.isBlank(argStr1)) {
      throw new DataWorkflowException(ERR_MSG);
    }
    Map<String, Object> requestMap = new HashMap<>();
    return new AviatorString(
        SignUtil.getInstance().createSign(requestMap, argStr1, String.valueOf(argStr2), argStr3));
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
    String argStr1 = (arg1.getValue(env) == null) ? "" : FunctionUtils.getStringValue(arg1, env);
    Long argStr2 =
        (arg2.getValue(env) == null) ? 0L : (Long) FunctionUtils.getNumberValue(arg2, env);
    String argStr3 = (arg3.getValue(env) == null) ? "" : FunctionUtils.getStringValue(arg3, env);
    if (StringUtils.isBlank(argStr1)) {
      throw new DataWorkflowException(ERR_MSG);
    }
    Map<String, Object> requestMap = new HashMap<>();
    return new AviatorString(
        SignUtil.getInstance().createSign(requestMap, argStr1, String.valueOf(argStr2), argStr3));
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
    String argStr1 = (arg1.getValue(env) == null) ? "" : FunctionUtils.getStringValue(arg1, env);
    Long argStr2 =
        (arg2.getValue(env) == null) ? 0L : (Long) FunctionUtils.getNumberValue(arg2, env);
    String argStr3 = (arg3.getValue(env) == null) ? "" : FunctionUtils.getStringValue(arg3, env);
    if (StringUtils.isBlank(argStr1)) {
      throw new DataWorkflowException(ERR_MSG);
    }
    Map<String, Object> requestMap = new HashMap<>();
    return new AviatorString(
        SignUtil.getInstance().createSign(requestMap, argStr1, String.valueOf(argStr2), argStr3));
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
    String argStr1 = (arg1.getValue(env) == null) ? "" : FunctionUtils.getStringValue(arg1, env);
    Long argStr2 =
        (arg2.getValue(env) == null) ? 0L : (Long) FunctionUtils.getNumberValue(arg2, env);
    String argStr3 = (arg3.getValue(env) == null) ? "" : FunctionUtils.getStringValue(arg3, env);
    if (StringUtils.isBlank(argStr1)) {
      throw new DataWorkflowException(ERR_MSG);
    }
    Map<String, Object> requestMap = new HashMap<>();
    return new AviatorString(
        SignUtil.getInstance().createSign(requestMap, argStr1, String.valueOf(argStr2), argStr3));
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
    String argStr1 = (arg1.getValue(env) == null) ? "" : FunctionUtils.getStringValue(arg1, env);
    Long argStr2 =
        (arg2.getValue(env) == null) ? 0L : (Long) FunctionUtils.getNumberValue(arg2, env);
    String argStr3 = (arg3.getValue(env) == null) ? "" : FunctionUtils.getStringValue(arg3, env);
    if (StringUtils.isBlank(argStr1)) {
      throw new DataWorkflowException(ERR_MSG);
    }
    Map<String, Object> requestMap = new HashMap<>();
    return new AviatorString(
        SignUtil.getInstance().createSign(requestMap, argStr1, String.valueOf(argStr2), argStr3));
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
    String argStr1 = (arg1.getValue(env) == null) ? "" : FunctionUtils.getStringValue(arg1, env);
    Long argStr2 =
        (arg2.getValue(env) == null) ? 0L : (Long) FunctionUtils.getNumberValue(arg2, env);
    String argStr3 = (arg3.getValue(env) == null) ? "" : FunctionUtils.getStringValue(arg3, env);
    if (StringUtils.isBlank(argStr1)) {
      throw new DataWorkflowException(ERR_MSG);
    }
    Map<String, Object> requestMap = new HashMap<>();
    return new AviatorString(
        SignUtil.getInstance().createSign(requestMap, argStr1, String.valueOf(argStr2), argStr3));
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
    String argStr1 = (arg1.getValue(env) == null) ? "" : FunctionUtils.getStringValue(arg1, env);
    Long argStr2 =
        (arg2.getValue(env) == null) ? 0L : (Long) FunctionUtils.getNumberValue(arg2, env);
    String argStr3 = (arg3.getValue(env) == null) ? "" : FunctionUtils.getStringValue(arg3, env);
    if (StringUtils.isBlank(argStr1)) {
      throw new DataWorkflowException(ERR_MSG);
    }
    Map<String, Object> requestMap = new HashMap<>();
    return new AviatorString(
        SignUtil.getInstance().createSign(requestMap, argStr1, String.valueOf(argStr2), argStr3));
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
    String argStr1 = (arg1.getValue(env) == null) ? "" : FunctionUtils.getStringValue(arg1, env);
    Long argStr2 =
        (arg2.getValue(env) == null) ? 0L : (Long) FunctionUtils.getNumberValue(arg2, env);
    String argStr3 = (arg3.getValue(env) == null) ? "" : FunctionUtils.getStringValue(arg3, env);
    if (StringUtils.isBlank(argStr1)) {
      throw new DataWorkflowException(ERR_MSG);
    }
    Map<String, Object> requestMap = new HashMap<>();
    return new AviatorString(
        SignUtil.getInstance().createSign(requestMap, argStr1, String.valueOf(argStr2), argStr3));
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
    String argStr1 = (arg1.getValue(env) == null) ? "" : FunctionUtils.getStringValue(arg1, env);
    Long argStr2 =
        (arg2.getValue(env) == null) ? 0L : (Long) FunctionUtils.getNumberValue(arg2, env);
    String argStr3 = (arg3.getValue(env) == null) ? "" : FunctionUtils.getStringValue(arg3, env);
    if (StringUtils.isBlank(argStr1)) {
      throw new DataWorkflowException(ERR_MSG);
    }
    Map<String, Object> requestMap = new HashMap<>();
    return new AviatorString(
        SignUtil.getInstance().createSign(requestMap, argStr1, String.valueOf(argStr2), argStr3));
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
    String argStr1 = (arg1.getValue(env) == null) ? "" : FunctionUtils.getStringValue(arg1, env);
    Long argStr2 =
        (arg2.getValue(env) == null) ? 0L : (Long) FunctionUtils.getNumberValue(arg2, env);
    String argStr3 = (arg3.getValue(env) == null) ? "" : FunctionUtils.getStringValue(arg3, env);
    if (StringUtils.isBlank(argStr1)) {
      throw new DataWorkflowException(ERR_MSG);
    }
    Map<String, Object> requestMap = new HashMap<>();
    return new AviatorString(
        SignUtil.getInstance().createSign(requestMap, argStr1, String.valueOf(argStr2), argStr3));
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
    String argStr1 = (arg1.getValue(env) == null) ? "" : FunctionUtils.getStringValue(arg1, env);
    Long argStr2 =
        (arg2.getValue(env) == null) ? 0L : (Long) FunctionUtils.getNumberValue(arg2, env);
    String argStr3 = (arg3.getValue(env) == null) ? "" : FunctionUtils.getStringValue(arg3, env);
    if (StringUtils.isBlank(argStr1)) {
      throw new DataWorkflowException(ERR_MSG);
    }
    Map<String, Object> requestMap = new HashMap<>();
    return new AviatorString(
        SignUtil.getInstance().createSign(requestMap, argStr1, String.valueOf(argStr2), argStr3));
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
    String argStr1 = (arg1.getValue(env) == null) ? "" : FunctionUtils.getStringValue(arg1, env);
    Long argStr2 =
        (arg2.getValue(env) == null) ? 0L : (Long) FunctionUtils.getNumberValue(arg2, env);
    String argStr3 = (arg3.getValue(env) == null) ? "" : FunctionUtils.getStringValue(arg3, env);
    if (StringUtils.isBlank(argStr1)) {
      throw new DataWorkflowException(ERR_MSG);
    }
    Map<String, Object> requestMap = new HashMap<>();
    return new AviatorString(
        SignUtil.getInstance().createSign(requestMap, argStr1, String.valueOf(argStr2), argStr3));
  }
}
