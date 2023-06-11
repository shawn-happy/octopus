package com.octopus.kettlex.core.row.record;

import com.octopus.kettlex.core.row.Record;
import com.octopus.kettlex.core.row.column.Column;

public class TerminateRecord implements Record {
  private static final TerminateRecord SINGLE = new TerminateRecord();

  private TerminateRecord() {}

  public static TerminateRecord get() {
    return SINGLE;
  }

  @Override
  public void addColumn(Column column) {}

  @Override
  public Column getColumn(int i) {
    return null;
  }

  @Override
  public int getColumnNumber() {
    return 0;
  }

  @Override
  public void setColumn(int i, Column column) {
    return;
  }
}
