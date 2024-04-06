package io.github.shawn.octopus.fluxus.engine.model.type;

import io.github.shawn.octopus.fluxus.api.model.type.DataWorkflowFieldType;
import java.time.LocalDate;
import java.time.LocalDateTime;
import java.time.LocalTime;
import lombok.Getter;

@Getter
public enum DateTimeFieldType implements DataWorkflowFieldType {
  DATE_TYPE("date", LocalDate.class),
  DATE_TIME_TYPE("dateTime", LocalDateTime.class),
  TIME_TYPE("time", LocalTime.class),
  ;

  private final Class<?> typeClass;
  private final String name;

  DateTimeFieldType(String name, Class<?> typeClass) {
    this.typeClass = typeClass;
    this.name = name;
  }

  @Override
  public String toString() {
    return name;
  }
}
