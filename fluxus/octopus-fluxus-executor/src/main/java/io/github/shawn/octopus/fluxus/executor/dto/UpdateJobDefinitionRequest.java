package io.github.shawn.octopus.fluxus.executor.dto;

import lombok.Getter;
import lombok.Setter;

@Getter
@Setter
public class UpdateJobDefinitionRequest {
  private String name;
  private String description;
}
