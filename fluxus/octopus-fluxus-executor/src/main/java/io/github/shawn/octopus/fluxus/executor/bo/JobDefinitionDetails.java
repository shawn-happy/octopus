package io.github.shawn.octopus.fluxus.executor.bo;

import lombok.Builder;
import lombok.Getter;

import java.util.List;

@Getter
@Builder
public class JobDefinitionDetails {
  private String jobId;
  private String name;
  private String description;
  private Version version;
  private Step source;
  private List<Step> transforms;
  private Step sink;
  private long createTime;
  private long updateTime;
}
