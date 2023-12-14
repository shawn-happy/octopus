package com.octopus.operators.engine.table;

import java.util.concurrent.atomic.AtomicLong;
import lombok.AllArgsConstructor;
import lombok.Getter;

public class EngineRow {

  private EngineRowRecord[] fields;
  private volatile long size;
  private final AtomicLong sizeIncrementor = new AtomicLong();

  public EngineRow(int rowSize) {
    this.fields = new EngineRowRecord[rowSize];
  }

  public EngineRowRecord[] getFields() {
    return fields;
  }

  @Getter
  @AllArgsConstructor
  public static class EngineRowRecord {
    private final Object field;
    private final FieldType fieldType;

    public static EngineRowRecord of(Object field, FieldType fieldType) {
      return new EngineRowRecord(field, fieldType);
    }
  }
}
