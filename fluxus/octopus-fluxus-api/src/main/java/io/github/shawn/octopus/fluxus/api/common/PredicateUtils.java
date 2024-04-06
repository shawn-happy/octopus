package io.github.shawn.octopus.fluxus.api.common;

import io.github.shawn.octopus.fluxus.api.exception.VerifyException;
import java.util.function.Supplier;

public abstract class PredicateUtils {

  public static boolean verify(boolean expression) {
    if (!expression) {
      throw new VerifyException("expression verify failed");
    }
    return true;
  }

  public static boolean verify(boolean expression, String errorMsg, Object... args) {
    if (!expression) {
      throw new VerifyException(String.format(errorMsg, args));
    }
    return true;
  }

  public static boolean verify(boolean expression, Supplier<String> errorMsg) {
    if (!expression) {
      throw new VerifyException(errorMsg.get());
    }
    return true;
  }

  public static boolean verify(boolean expression, String errorMsg, Throwable e, Object... args) {
    if (!expression) {
      throw new VerifyException(String.format(errorMsg, args), e);
    }
    return true;
  }

  public static boolean verify(
      boolean expression, Supplier<String> errorMsg, Supplier<Throwable> e) {
    if (!expression) {
      throw new VerifyException(errorMsg.get(), e.get());
    }
    return true;
  }
}
