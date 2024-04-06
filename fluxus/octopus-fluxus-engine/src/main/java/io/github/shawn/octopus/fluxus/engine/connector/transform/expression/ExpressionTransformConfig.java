package io.github.shawn.octopus.fluxus.engine.connector.transform.expression;

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
public class ExpressionTransformConfig
    extends BaseTransformConfig<ExpressionTransformConfig.ExpressionOptions>
    implements TransformConfig<ExpressionTransformConfig.ExpressionOptions> {

  @Builder.Default private String identifier = Constants.TransformConstants.EXPRESSION_TRANSFORM;
  private String id;
  private String name;
  private List<String> inputs;
  private String output;
  private ExpressionTransformConfig.ExpressionOptions options;

  @Override
  protected void checkOptions() {
    verify(ArrayUtils.isNotEmpty(options.expressionFields), "expressionFields cannot be null");
  }

  @Override
  protected void loadTransformConfig(String json) {
    ExpressionTransformConfig expressionTransformConfig =
        JsonUtils.fromJson(json, new TypeReference<ExpressionTransformConfig>() {})
            .orElseThrow(
                () -> new DataWorkflowException("expression transform config deserialize error"));
    this.id =
        isNotBlank(expressionTransformConfig.getId())
            ? expressionTransformConfig.getId()
            : IdGenerator.uuid();
    this.name = expressionTransformConfig.getName();
    this.inputs = expressionTransformConfig.getInputs();
    this.output = expressionTransformConfig.getOutput();
    this.options = expressionTransformConfig.getOptions();
  }

  @Getter
  @Builder
  @NoArgsConstructor
  @AllArgsConstructor
  public static class ExpressionOptions implements TransformConfig.TransformOptions {
    private ExpressionField[] expressionFields;
  }

  @Getter
  @Builder
  @NoArgsConstructor
  @AllArgsConstructor
  public static class ExpressionField {
    private String expression;
    private String destination;
    private String type;
  }
}
