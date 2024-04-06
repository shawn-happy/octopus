package io.github.shawn.octopus.fluxus.api.config;

import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Getter;
import lombok.NoArgsConstructor;

@Getter
@Builder
@NoArgsConstructor
@AllArgsConstructor
public class Column {
  private String name;
  private String type;
  private boolean nullable;
  private Integer length;
  private String comment;
  private Object defaultValue;
}
