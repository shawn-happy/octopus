package io.github.shawn.octopus.fluxus.executor.bo.stepOptions.transform;

import static io.github.shawn.octopus.fluxus.engine.common.Constants.TransformConstants.DESENSITIZE_PARSE_TRANSFORM;

import io.github.shawn.octopus.fluxus.api.config.PluginType;
import io.github.shawn.octopus.fluxus.executor.bo.StepAttribute;
import io.github.shawn.octopus.fluxus.executor.bo.StepOptions;
import com.fasterxml.jackson.annotation.JsonTypeName;
import java.util.Arrays;
import java.util.List;

import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Getter;
import lombok.NoArgsConstructor;

@Getter
@Builder
@JsonTypeName("transform_desensitize")
@NoArgsConstructor
@AllArgsConstructor
public class DesensitizeStepOptions implements StepOptions {
  @Builder.Default private final PluginType pluginType = PluginType.TRANSFORM;
  @Builder.Default private final String identify = DESENSITIZE_PARSE_TRANSFORM;

  private String field;
  private String ruleId;
  private String dataType;
  private String rule;
  private String config;

  @Override
  public List<StepAttribute> getStepAttributes() {
    return Arrays.asList(
        StepAttribute.builder().code("field").value(getField()).build(),
        StepAttribute.builder().code("ruleId").value(getRuleId()).build(),
        StepAttribute.builder().code("dataType").value(getDataType()).build(),
        StepAttribute.builder().code("rule").value(getRule()).build(),
        StepAttribute.builder().code("config").value(getConfig()).build());
  }
}
