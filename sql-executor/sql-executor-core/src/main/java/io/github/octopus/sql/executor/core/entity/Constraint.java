package io.github.octopus.sql.executor.core.entity;

import java.util.List;
import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Getter;
import lombok.NoArgsConstructor;

@Getter
@Builder
@NoArgsConstructor
@AllArgsConstructor
public class Constraint {
  private String name;
  private String type;
  private List<String> constraints;
}
