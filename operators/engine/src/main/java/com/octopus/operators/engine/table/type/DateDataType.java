package com.octopus.operators.engine.table.type;

import java.time.LocalDate;
import java.time.LocalDateTime;
import java.time.LocalTime;

public enum DateDataType implements RowDataType {
  DATE_TYPE(LocalDate.class, "date"),
  DATE_TIME_TYPE(LocalDateTime.class, "dateTime"),
  TIME_TYPE(LocalTime.class, "time"),
  ;
  private final Class<?> dateClass;
  private final String name;

  DateDataType(Class<?> dateClass, String name) {
    this.dateClass = dateClass;
    this.name = name;
  }

  @Override
  public Class<?> getTypeClass() {
    return dateClass;
  }

  @Override
  public String toString() {
    return name;
  }
}
