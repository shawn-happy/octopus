package com.octopus.operators.engine.exception;

import com.octopus.operators.engine.util.MessageFormatUtils;

public class TableNotFoundException extends DataTunnelException{

  private static final String TABLE_NOT_FOUND_MSG = "Table {} Not Found.";

  public TableNotFoundException(String table) {
    super(MessageFormatUtils.getMessage(TABLE_NOT_FOUND_MSG, table));
  }
}
