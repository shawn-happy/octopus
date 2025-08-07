package com.octopus.operators.engine.util;

import org.slf4j.helpers.MessageFormatter;

public class MessageFormatUtils {

  public static String getMessage(String msg, String... placeholders) {
    return MessageFormatter.arrayFormat(msg, placeholders).getMessage();
  }
}
