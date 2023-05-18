package com.octopus.kettlex.core.row;

import com.octopus.kettlex.core.row.column.Column;

public interface Record {

  void addColumn(Column column);

  void setColumn(int i, final Column column);

  Column getColumn(int i);

  String toString();

  int getColumnNumber();
}
