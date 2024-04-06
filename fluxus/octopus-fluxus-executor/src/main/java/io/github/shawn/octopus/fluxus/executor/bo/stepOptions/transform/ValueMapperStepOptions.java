package io.github.shawn.octopus.fluxus.executor.bo.stepOptions.transform;

import static io.github.shawn.octopus.fluxus.engine.common.Constants.TransformConstants.VALUE_MAPPER_TRANSFORM;

import io.github.shawn.octopus.fluxus.api.common.JsonUtils;
import io.github.shawn.octopus.fluxus.api.config.PluginType;
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
@JsonTypeName("transform_valueMapper")
@NoArgsConstructor
@AllArgsConstructor
public class ValueMapperStepOptions implements StepOptions {
  @Builder.Default private final PluginType pluginType = PluginType.TRANSFORM;
  @Builder.Default private final String identify = VALUE_MAPPER_TRANSFORM;

  private ValueMapper[] valueMappers;

  @Override
  public List<StepAttribute> getStepAttributes() {
    return Collections.singletonList(
        StepAttribute.builder()
            .code("valueMappers")
            .value(JsonUtils.toJsonString(valueMappers))
            .build());
  }

  @Getter
  @Builder
  @NoArgsConstructor
  @AllArgsConstructor
  public static class ValueMapper {
    private String fieldName;
    private Object[] sourceValues;
    private Object[] targetValues;
  }
}
