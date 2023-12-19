package com.octopus.operators.engine.table.type;

import java.time.LocalDate;
import java.time.LocalDateTime;
import java.time.LocalTime;

public enum DateDataType implements RowDataType {
  DATE_TYPE(LocalDate.class),
  DATE_TIME_TYPE(LocalDateTime.class),
  TIME_TYPE(LocalTime.class),
  ;
  private final Class<?> dateClass;

  DateDataType(Class<?> dateClass) {
    this.dateClass = dateClass;
  }

  @Override
  public Class<?> getTypeClass() {
    return dateClass;
  }
}
