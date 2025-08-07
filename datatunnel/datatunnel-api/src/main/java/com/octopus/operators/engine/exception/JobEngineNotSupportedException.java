package com.octopus.operators.engine.exception;

import com.octopus.operators.engine.util.MessageFormatUtils;

public class JobEngineNotSupportedException extends DataTunnelException {

  private static final String ENGINE_NOT_FOUND_MSG = "The Job Engine {} Not Supported";

  public JobEngineNotSupportedException(String jobEngine) {
    super(MessageFormatUtils.getMessage(ENGINE_NOT_FOUND_MSG, jobEngine));
  }
}
