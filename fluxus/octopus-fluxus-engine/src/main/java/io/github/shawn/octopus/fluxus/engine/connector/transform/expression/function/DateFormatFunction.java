package io.github.shawn.octopus.fluxus.engine.connector.transform.expression.function;

import com.googlecode.aviator.runtime.function.AbstractFunction;
import com.googlecode.aviator.runtime.function.FunctionUtils;
import com.googlecode.aviator.runtime.type.AviatorObject;
import com.googlecode.aviator.runtime.type.AviatorString;
import io.github.shawn.octopus.fluxus.api.exception.DataWorkflowException;
import java.text.ParseException;
import java.util.Date;
import java.util.Map;
import org.apache.commons.lang3.StringUtils;
import org.apache.commons.lang3.time.DateFormatUtils;
import org.apache.commons.lang3.time.DateUtils;

public class DateFormatFunction extends AbstractFunction {

  private static final String[] DATE_FORMAT =
      new String[] {
        "yyyy-MM-dd HH:mm:ss",
        "yyyy/MM/dd HH:mm:ss",
        "yyyy.MM.dd HH:mm:ss",
        "yyyy-MM-dd",
        "yyyy/MM/dd",
        "yyyy.MM.dd",
        "HH:mm:ss",
        "yyyy-MM-dd HH:mm",
        "yyyy-MM-dd HH:mm:ss.SSS"
      };

  @Override
  public String getName() {
    return "date.format";
  }

  /**
   * 对当前时间格式化
   *
   * @param env env
   * @param arg1 模板
   * @return 格式化结果
   */
  @Override
  public AviatorObject call(Map<String, Object> env, AviatorObject arg1) {
    String argStr1 = (arg1.getValue(env) == null) ? "" : FunctionUtils.getStringValue(arg1, env);
    return new AviatorString(DateFormatUtils.format(new Date(), argStr1));
  }

  /** 对给定的字符串进行时间格式化 将时间arg2格式化为arg1 arg2为空时输出处理时日期 */
  @Override
  public AviatorObject call(Map<String, Object> env, AviatorObject arg1, AviatorObject arg2) {
    String argStr1 = (arg1.getValue(env) == null) ? "" : FunctionUtils.getStringValue(arg1, env);
    String argStr2 = (arg2.getValue(env) == null) ? "" : FunctionUtils.getStringValue(arg2, env);
    Date parse;
    String format;
    if (StringUtils.isBlank(argStr2)) {
      parse = new Date();
    } else {
      try {
        parse = DateUtils.parseDate(argStr2, DATE_FORMAT);
      } catch (ParseException e) {
        throw new DataWorkflowException(String.format("parseDate[%s] Exception", argStr2));
      }
    }
    format = DateFormatUtils.format(parse, argStr1);
    return new AviatorString(format);
  }

  /**
   * 对当前时间按指定参数进行格式化
   *
   * @param env env
   * @param arg1 格式化模板
   * @param arg2 参数标记
   * @param arg3 时间偏移量
   * @return 格式化结果
   */
  @Override
  public AviatorObject call(
      Map<String, Object> env, AviatorObject arg1, AviatorObject arg2, AviatorObject arg3) {
    String argStr1 = (arg1.getValue(env) == null) ? "" : FunctionUtils.getStringValue(arg1, env);
    String argStr2 = (arg2.getValue(env) == null) ? "" : FunctionUtils.getStringValue(arg2, env);
    String argStr3 = (arg3.getValue(env) == null) ? "" : FunctionUtils.getStringValue(arg3, env);
    int days;
    String format = "";
    if ("prev".equalsIgnoreCase(argStr2)) {
      days = -Integer.parseInt(argStr3);
      Date dateTime = DateUtils.addDays(new Date(), days);
      format = DateFormatUtils.format(dateTime, argStr1);
    } else if ("next".equalsIgnoreCase(argStr2)) {
      days = Integer.parseInt(argStr3);
      Date dateTime = DateUtils.addDays(new Date(), days);
      format = DateFormatUtils.format(dateTime, argStr1);
    } else if ("long".equalsIgnoreCase(argStr2)) {
      Date date = new Date(Long.parseLong(argStr3));
      format = DateFormatUtils.format(date, argStr1);
    }
    return new AviatorString(format);
  }
}
