package com.octopus.operators.engine.exception;

import com.octopus.operators.engine.util.MessageFormatUtils;

public class TableAlreadyExistsException extends DataTunnelException {

  private static final String TABLE_ALREADY_EXISTS_MSG = "Table {} Already Exists";

  public TableAlreadyExistsException(String table) {
    super(MessageFormatUtils.getMessage(TABLE_ALREADY_EXISTS_MSG, table));
  }
}
