package io.github.shawn.octopus.fluxus.executor.vo;

import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Getter;
import lombok.NoArgsConstructor;

@Getter
@Builder
@NoArgsConstructor
@AllArgsConstructor
public class JobDefinitionVO {
  private String jobId;
  private String name;
  private String description;
  private String version;
}
