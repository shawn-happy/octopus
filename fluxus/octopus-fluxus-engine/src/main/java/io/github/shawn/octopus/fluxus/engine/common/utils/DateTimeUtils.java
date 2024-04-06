package io.github.shawn.octopus.fluxus.engine.common.utils;

import java.time.LocalDateTime;
import java.time.format.DateTimeFormatter;
import java.util.HashMap;
import java.util.Map;
import lombok.Getter;

public class DateTimeUtils {
  private static final Map<Formatter, DateTimeFormatter> FORMATTER_MAP = new HashMap<>();

  static {
    FORMATTER_MAP.put(
        Formatter.YYYY_MM_DD_HH_MM_SS,
        DateTimeFormatter.ofPattern(Formatter.YYYY_MM_DD_HH_MM_SS.value));
    FORMATTER_MAP.put(
        Formatter.YYYY_MM_DD_HH_MM_SS_SPOT,
        DateTimeFormatter.ofPattern(Formatter.YYYY_MM_DD_HH_MM_SS_SPOT.value));
    FORMATTER_MAP.put(
        Formatter.YYYY_MM_DD_HH_MM_SS_SLASH,
        DateTimeFormatter.ofPattern(Formatter.YYYY_MM_DD_HH_MM_SS_SLASH.value));
    FORMATTER_MAP.put(
        Formatter.YYYY_MM_DD_HH_MM_SS_NO_SPLIT,
        DateTimeFormatter.ofPattern(Formatter.YYYY_MM_DD_HH_MM_SS_NO_SPLIT.value));
  }

  public static LocalDateTime parse(String dateTime, Formatter formatter) {
    return LocalDateTime.parse(dateTime, FORMATTER_MAP.get(formatter));
  }

  public static String toString(LocalDateTime dateTime, Formatter formatter) {
    return dateTime.format(FORMATTER_MAP.get(formatter));
  }

  @Getter
  public enum Formatter {
    YYYY_MM_DD_HH_MM_SS("yyyy-MM-dd HH:mm:ss"),
    YYYY_MM_DD_HH_MM_SS_SPOT("yyyy.MM.dd HH:mm:ss"),
    YYYY_MM_DD_HH_MM_SS_SLASH("yyyy/MM/dd HH:mm:ss"),
    YYYY_MM_DD_HH_MM_SS_NO_SPLIT("yyyyMMddHHmmss");

    private final String value;

    Formatter(String value) {
      this.value = value;
    }

    public static Formatter parse(String format) {
      Formatter[] formatters = Formatter.values();
      for (Formatter formatter : formatters) {
        if (formatter.getValue().equals(format)) {
          return formatter;
        }
      }
      String errorMsg = String.format("Illegal format [%s]", format);
      throw new IllegalArgumentException(errorMsg);
    }
  }
}
