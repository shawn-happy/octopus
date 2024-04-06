package io.github.shawn.octopus.fluxus.executor.bo.stepOptions.transform;

import static io.github.shawn.octopus.fluxus.engine.common.Constants.TransformConstants.GENERATED_FILED_TRANSFORM;

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
@JsonTypeName("transform_generatedField")
@NoArgsConstructor
@AllArgsConstructor
public class GeneratedFieldStepOptions implements StepOptions {
  @Builder.Default private final PluginType pluginType = PluginType.TRANSFORM;
  @Builder.Default private final String identify = GENERATED_FILED_TRANSFORM;

  private GeneratedField[] fields;

  @Override
  public List<StepAttribute> getStepAttributes() {
    return Collections.singletonList(
        StepAttribute.builder().code("fields").value(JsonUtils.toJsonString(getFields())).build());
  }

  @Getter
  @Builder
  @NoArgsConstructor
  @AllArgsConstructor
  public static class GeneratedField {
    private GenerateAlgo algo;
    private String field;
    private ColumnType type;
  }

  public static enum GenerateAlgo {
    CURRENT_TIME,
    UUID,
    SNOW_ID
  }
}
