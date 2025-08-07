package com.octopus.operators.engine.exception;

import com.octopus.operators.engine.util.MessageFormatUtils;

public class ConfigParseException extends DataTunnelException {

  private static final String MSG = "config parse error. config:\n {}";

  public ConfigParseException(String config, Throwable e) {
    super(MessageFormatUtils.getMessage(MSG, config), e);
  }

  public ConfigParseException(Throwable e) {
    super("config parse error", e);
  }
}
