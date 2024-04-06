package io.github.shawn.octopus.fluxus.api.common;

import io.github.shawn.octopus.fluxus.api.exception.DataWorkflowException;
import java.util.function.Supplier;

@FunctionalInterface
public interface ThrowableFunction<T, R> {

  R apply(T t) throws Throwable;

  default R execute(T t) throws RuntimeException {
    return execute(t, () -> new RuntimeException("function execute error"));
  }

  default R execute(T t, Supplier<Throwable> supplier) throws RuntimeException {
    R result = null;
    try {
      result = apply(t);
    } catch (Throwable e) {
      throw new DataWorkflowException(supplier.get().getMessage(), supplier.get().initCause(e));
    }
    return result;
  }

  static <T, R> R execute(T t, ThrowableFunction<T, R> function) {
    return function.execute(t);
  }

  static <T, R> R execute(T t, ThrowableFunction<T, R> function, Supplier<Throwable> supplier) {
    return function.execute(t, supplier);
  }
}
