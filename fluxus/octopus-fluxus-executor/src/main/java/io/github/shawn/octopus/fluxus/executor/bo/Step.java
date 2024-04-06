package io.github.shawn.octopus.fluxus.executor.bo;

import io.github.shawn.octopus.fluxus.api.config.PluginType;
import java.util.List;
import lombok.Builder;
import lombok.Getter;

@Builder
@Getter
public class Step {
  private String id;
  private String jobId;
  private String name;
  private PluginType pluginType;
  private String identify;
  private String description;
  private List<StepAttribute> attributes;
  private List<String> input;
  private String output;
  private long createTime;
  private long updateTime;
}
