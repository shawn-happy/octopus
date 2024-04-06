package io.github.shawn.octopus.fluxus.executor.dto;

import lombok.Getter;
import lombok.Setter;

@Getter
@Setter
public class JobDefinitionRequest {
  private String jobName;
  private String description;
}
