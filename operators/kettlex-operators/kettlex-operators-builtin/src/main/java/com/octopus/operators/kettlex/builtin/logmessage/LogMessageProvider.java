package com.octopus.operators.kettlex.builtin.logmessage;

import com.octopus.operators.kettlex.core.provider.WriterProvider;

public class LogMessageProvider implements WriterProvider<LogMessage, LogMessageConfig> {
  @Override
  public String getType() {
    return "log-message";
  }

  @Override
  public Class<LogMessage> getWriter() {
    return LogMessage.class;
  }

  @Override
  public Class<LogMessageConfig> getWriterConfig() {
    return LogMessageConfig.class;
  }
}
