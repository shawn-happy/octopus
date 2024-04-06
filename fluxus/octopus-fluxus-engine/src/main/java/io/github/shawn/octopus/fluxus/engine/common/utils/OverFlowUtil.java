package io.github.shawn.octopus.fluxus.engine.common.utils;

import io.github.shawn.octopus.fluxus.api.exception.DataWorkflowException;
import java.math.BigDecimal;
import java.math.BigInteger;

public final class OverFlowUtil {

  private OverFlowUtil() {}

  private static final Integer MAX_INTEGER = Integer.MAX_VALUE;
  private static final Integer MIN_INTEGER = Integer.MIN_VALUE;

  public static final BigInteger MAX_LONG = BigInteger.valueOf(Long.MAX_VALUE);

  public static final BigInteger MIN_LONG = BigInteger.valueOf(Long.MIN_VALUE);

  public static final BigDecimal MIN_DOUBLE_POSITIVE =
      new BigDecimal(String.valueOf(Double.MIN_VALUE));

  public static final BigDecimal MAX_DOUBLE_POSITIVE =
      new BigDecimal(String.valueOf(Double.MAX_VALUE));

  public static boolean isLongOverflow(final BigInteger integer) {
    return (integer.compareTo(OverFlowUtil.MAX_LONG) > 0
        || integer.compareTo(OverFlowUtil.MIN_LONG) < 0);
  }

  public static boolean isIntegerOverflow(final Long integer) {
    if (integer == null) {
      return false;
    }
    return MAX_INTEGER >= integer || MAX_INTEGER <= integer;
  }

  public static void validateIntegerNotOverFlow(final Long num) {
    boolean isOverFlow = OverFlowUtil.isIntegerOverflow(num);

    if (isOverFlow) {
      throw new DataWorkflowException(String.format("[%s] convert to integer overflow .", num));
    }
  }

  public static void validateLongNotOverFlow(final BigInteger integer) {
    boolean isOverFlow = OverFlowUtil.isLongOverflow(integer);

    if (isOverFlow) {
      throw new DataWorkflowException(
          String.format("[%s] convert to long overflow .", integer.toString()));
    }
  }

  public static boolean isDoubleOverFlow(final BigDecimal decimal) {
    if (decimal.signum() == 0) {
      return false;
    }

    BigDecimal newDecimal = decimal;
    boolean isPositive = decimal.signum() == 1;
    if (!isPositive) {
      newDecimal = decimal.negate();
    }

    return (newDecimal.compareTo(MIN_DOUBLE_POSITIVE) < 0
        || newDecimal.compareTo(MAX_DOUBLE_POSITIVE) > 0);
  }

  public static void validateDoubleNotOverFlow(final BigDecimal decimal) {
    boolean isOverFlow = OverFlowUtil.isDoubleOverFlow(decimal);
    if (isOverFlow) {
      throw new DataWorkflowException(
          String.format("[%s] convert to double overflow. ", decimal.toPlainString()));
    }
  }
}
