package com.octopus.kettlex.core.row.record;

import com.octopus.kettlex.core.exception.KettleXException;
import com.octopus.kettlex.core.row.Record;
import com.octopus.kettlex.core.row.column.Column;
import com.octopus.kettlex.core.utils.JsonUtil;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

public class DefaultRecord implements Record {
  private List<Column> columns;

  public DefaultRecord() {
    columns = new ArrayList<>(2 << 4);
  }

  @Override
  public void addColumn(Column column) {
    columns.add(column);
  }

  @Override
  public void setColumn(int i, Column column) {
    if (i < 0) {
      throw new KettleXException("index cannot be less than 0");
    }

    if (i >= columns.size()) {
      expandCapacity(i + 1);
    }

    this.columns.set(i, column);
  }

  @Override
  public Column getColumn(int i) {
    if (i < 0 || i >= columns.size()) {
      return null;
    }
    return columns.get(i);
  }

  @Override
  public int getColumnNumber() {
    return this.columns.size();
  }

  @Override
  public String toString() {
    Map<String, Object> json = new HashMap<String, Object>();
    json.put("size", this.getColumnNumber());
    json.put("data", this.columns);
    return JsonUtil.toJson(json).orElse(null);
  }

  private void expandCapacity(int totalSize) {
    if (totalSize <= 0) {
      return;
    }

    int needToExpand = totalSize - columns.size();
    while (needToExpand-- > 0) {
      this.columns.add(null);
    }
  }
}
