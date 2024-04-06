package io.github.shawn.octopus.fluxus.executor.bo.stepOptions.transform;

import static io.github.shawn.octopus.fluxus.engine.common.Constants.TransformConstants.EXPRESSION_TRANSFORM;

import io.github.shawn.octopus.fluxus.api.common.JsonUtils;
import io.github.shawn.octopus.fluxus.api.config.PluginType;
import io.github.shawn.octopus.fluxus.executor.bo.ColumnType;
import io.github.shawn.octopus.fluxus.executor.bo.StepAttribute;
import io.github.shawn.octopus.fluxus.executor.bo.StepOptions;
import com.fasterxml.jackson.annotation.JsonTypeName;
import java.util.Collections;
import java.util.List;

import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Getter;
import lombok.NoArgsConstructor;

@Getter
@Builder
@JsonTypeName("transform_expression")
@NoArgsConstructor
@AllArgsConstructor
public class ExpressionStepOptions implements StepOptions {
  @Builder.Default private final PluginType pluginType = PluginType.TRANSFORM;
  @Builder.Default private final String identify = EXPRESSION_TRANSFORM;

  private ExpressionParams[] expressionParams;

  @Override
  public List<StepAttribute> getStepAttributes() {
    return Collections.singletonList(
        StepAttribute.builder()
            .code("expressionParams")
            .value(JsonUtils.toJsonString(expressionParams))
            .build());
  }

  @Getter
  @Builder
  @NoArgsConstructor
  @AllArgsConstructor
  public static class ExpressionParams {
    private String expression;
    private String result;
    private ColumnType type;
  }
}
