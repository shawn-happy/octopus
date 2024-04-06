package io.github.shawn.octopus.fluxus.engine.connector.transform.generatedField;

import static io.github.shawn.octopus.fluxus.api.common.PredicateUtils.verify;
import static org.apache.commons.lang3.StringUtils.isNotBlank;

import com.fasterxml.jackson.core.type.TypeReference;
import io.github.shawn.octopus.fluxus.api.common.IdGenerator;
import io.github.shawn.octopus.fluxus.api.common.JsonUtils;
import io.github.shawn.octopus.fluxus.api.config.BaseTransformConfig;
import io.github.shawn.octopus.fluxus.api.config.TransformConfig;
import io.github.shawn.octopus.fluxus.api.exception.DataWorkflowException;
import io.github.shawn.octopus.fluxus.engine.common.Constants;
import java.util.List;
import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Getter;
import lombok.NoArgsConstructor;
import org.apache.commons.lang3.ArrayUtils;

@Getter
@Builder
@NoArgsConstructor
@AllArgsConstructor
public class GeneratedFieldTransformConfig
    extends BaseTransformConfig<GeneratedFieldTransformConfig.GeneratedFieldTransformOptions>
    implements TransformConfig<GeneratedFieldTransformConfig.GeneratedFieldTransformOptions> {

  @Builder.Default
  private String identifier = Constants.TransformConstants.GENERATED_FILED_TRANSFORM;

  private String id;

  private String name;

  private List<String> inputs;

  private String output;

  private GeneratedFieldTransformConfig.GeneratedFieldTransformOptions options;

  @Override
  protected void checkOptions() {
    verify(
        ArrayUtils.isNotEmpty(options.getFields()),
        "Fields in generatedFiled options cannot be null");
  }

  @Override
  protected void loadTransformConfig(String json) {
    GeneratedFieldTransformConfig generatedFieldTransformConfig =
        JsonUtils.fromJson(json, new TypeReference<GeneratedFieldTransformConfig>() {})
            .orElseThrow(
                () ->
                    new DataWorkflowException("generatedField transform config deserialize error"));
    this.id =
        isNotBlank(generatedFieldTransformConfig.getId())
            ? generatedFieldTransformConfig.getId()
            : IdGenerator.uuid();
    this.name = generatedFieldTransformConfig.getName();
    this.inputs = generatedFieldTransformConfig.getInputs();
    this.output = generatedFieldTransformConfig.getOutput();
    this.options = generatedFieldTransformConfig.getOptions();
  }

  @Getter
  @Builder
  @NoArgsConstructor
  @AllArgsConstructor
  public static class GeneratedFieldTransformOptions implements TransformConfig.TransformOptions {

    private Field[] fields;

    @Getter
    @Builder
    @NoArgsConstructor
    @AllArgsConstructor
    public static class Field {
      private GenerateType generateType;
      private String destName;
    }
  }
}
