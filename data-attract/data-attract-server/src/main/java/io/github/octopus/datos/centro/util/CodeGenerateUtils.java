package io.github.octopus.datos.centro.util;

import io.github.octopus.datos.centro.common.exception.DataCenterServiceException;
import java.net.InetAddress;
import java.net.UnknownHostException;
import java.util.Objects;

public class CodeGenerateUtils {
  // start timestamp
  private static final long START_TIMESTAMP = 1609430400000L; // 2021-01-01 00:00:00
  // Each machine generates 32 in the same millisecond
  private static final long LOW_DIGIT_BIT = 5L;
  private static final long MIDDLE_BIT = 2L;
  private static final long MAX_LOW_DIGIT = ~(-1L << LOW_DIGIT_BIT);
  // The displacement to the left
  private static final long MIDDLE_LEFT = LOW_DIGIT_BIT;
  private static final long HIGH_DIGIT_LEFT = LOW_DIGIT_BIT + MIDDLE_BIT;
  private final long machineHash;
  private long lowDigit = 0L;
  private long recordMillisecond = -1L;

  private static final long SYSTEM_TIMESTAMP = System.currentTimeMillis();
  private static final long SYSTEM_NANOTIME = System.nanoTime();

  private CodeGenerateUtils() throws DataCenterServiceException {
    try {
      this.machineHash =
          Math.abs(Objects.hash(InetAddress.getLocalHost().getHostName()))
              % (2 << (MIDDLE_BIT - 1));
    } catch (UnknownHostException e) {
      throw new DataCenterServiceException(e.getMessage());
    }
  }

  private static CodeGenerateUtils instance = null;

  public static synchronized CodeGenerateUtils getInstance() throws DataCenterServiceException {
    if (instance == null) {
      instance = new CodeGenerateUtils();
    }
    return instance;
  }

  public synchronized String genCode(String prefix) throws DataCenterServiceException {
    long code = genCode();
    return String.format("%s_%d", prefix, code);
  }

  public synchronized long genCode() throws DataCenterServiceException {
    long nowtMillisecond = systemMillisecond();
    if (nowtMillisecond < recordMillisecond) {
      throw new DataCenterServiceException("New code exception because time is set back.");
    }
    if (nowtMillisecond == recordMillisecond) {
      lowDigit = (lowDigit + 1) & MAX_LOW_DIGIT;
      if (lowDigit == 0L) {
        while (nowtMillisecond <= recordMillisecond) {
          nowtMillisecond = systemMillisecond();
        }
      }
    } else {
      lowDigit = 0L;
    }
    recordMillisecond = nowtMillisecond;
    return (nowtMillisecond - START_TIMESTAMP) << HIGH_DIGIT_LEFT
        | machineHash << MIDDLE_LEFT
        | lowDigit;
  }

  private long systemMillisecond() {
    return SYSTEM_TIMESTAMP + (System.nanoTime() - SYSTEM_NANOTIME) / 1000000;
  }
}
