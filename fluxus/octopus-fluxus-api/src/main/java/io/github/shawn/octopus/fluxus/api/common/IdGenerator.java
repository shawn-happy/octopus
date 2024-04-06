package io.github.shawn.octopus.fluxus.api.common;

import java.util.UUID;
import java.util.concurrent.atomic.AtomicLong;

public class IdGenerator {

  private final AtomicLong longAdder = new AtomicLong();

  public long getNextId() {
    return longAdder.incrementAndGet();
  }

  public static String uuid() {
    return UUID.randomUUID().toString().replace("-", "");
  }
}
