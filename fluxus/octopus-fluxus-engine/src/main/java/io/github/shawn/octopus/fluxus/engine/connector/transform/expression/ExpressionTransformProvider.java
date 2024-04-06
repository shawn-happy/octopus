package io.github.shawn.octopus.fluxus.engine.connector.transform.expression;

import io.github.shawn.octopus.fluxus.api.config.TransformConfig;
import io.github.shawn.octopus.fluxus.api.provider.TransformProvider;
import io.github.shawn.octopus.fluxus.engine.common.Constants;

public class ExpressionTransformProvider implements TransformProvider<ExpressionTransformConfig> {

  @Override
  public String getIdentifier() {
    return Constants.TransformConstants.EXPRESSION_TRANSFORM;
  }

  @Override
  public ExpressionTransformConfig getTransformConfig() {
    return new ExpressionTransformConfig();
  }

  @Override
  public ExpressionTransform getTransform(TransformConfig<?> transformConfig) {
    return new ExpressionTransform((ExpressionTransformConfig) transformConfig);
  }
}
