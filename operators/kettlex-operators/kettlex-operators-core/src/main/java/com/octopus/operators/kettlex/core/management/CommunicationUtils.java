package com.octopus.operators.kettlex.core.management;

import org.jetbrains.annotations.NotNull;

public final class CommunicationUtils {
  private CommunicationUtils() {}

  public static Communication getLastCommunication(
      @NotNull Communication now, @NotNull Communication old) {
    long totalReadRecords = now.getSendRecords();
    long timeInterval = now.getTimestamp() - old.getTimestamp();
    long sec = timeInterval <= 1000 ? 1 : timeInterval / 1000;
    long recordsSpeed = (totalReadRecords - old.getTransformRecords()) / sec;
    now.setProcessSpeed(recordsSpeed < 0 ? 0 : recordsSpeed);
    if (old.getException() != null) {
      now.setException(old.getException());
    }
    return now;
  }
}
