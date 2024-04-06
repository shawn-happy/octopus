package io.github.shawn.octopus.fluxus.executor.bo;

import lombok.Builder;
import lombok.Getter;

@Getter
@Builder
public class JobDefinitionInfo {
  private String jobId;
  private String name;
  private String description;
  private Version version;
  private long createTime;
  private long updateTime;
}
