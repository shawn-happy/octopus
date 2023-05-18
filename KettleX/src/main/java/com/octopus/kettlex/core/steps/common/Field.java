package com.octopus.kettlex.core.steps.common;

import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Getter;
import lombok.NoArgsConstructor;
import lombok.Setter;

@Getter
@Setter
@Builder
@NoArgsConstructor
@AllArgsConstructor
public class Field {

  private String name;
  private String type;
  private Object value;
  private String format;
  private Integer length;
}
