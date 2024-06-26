package io.github.shawn.octopus.fluxus.engine.connector.sink.doris.rest;

import java.util.regex.Pattern;

public class DorisRestUtils {

  public static final Pattern LABEL_EXIST_PATTERN =
      Pattern.compile(
          "errCode = 2, detailMessage = Label \\[(.*)\\] "
              + "has already been used, relate to txn \\[(\\d+)\\]");
  public static final Pattern COMMITTED_PATTERN =
      Pattern.compile(
          "errCode = 2, detailMessage = transaction \\[(\\d+)\\] "
              + "is already \\b(COMMITTED|committed|VISIBLE|visible)\\b, not pre-committed.");

  public static boolean isCommitted(String msg) {
    return COMMITTED_PATTERN.matcher(msg).matches();
  }
}
