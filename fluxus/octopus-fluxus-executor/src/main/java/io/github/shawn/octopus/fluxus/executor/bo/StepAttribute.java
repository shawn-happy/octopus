package io.github.shawn.octopus.fluxus.executor.bo;

import lombok.Builder;
import lombok.Getter;

@Builder
@Getter
public class StepAttribute {
  private String id;
  private String jobId;
  private String stepId;
  private String code;
  private Object value;
  private long createTime;
  private long updateTime;
}
