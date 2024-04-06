package io.github.shawn.octopus.fluxus.engine.connector.transform.generatedField;

import io.github.shawn.octopus.fluxus.api.config.TransformConfig;
import io.github.shawn.octopus.fluxus.api.provider.TransformProvider;
import io.github.shawn.octopus.fluxus.engine.common.Constants;

public class GeneratedFieldTransformProvider
    implements TransformProvider<GeneratedFieldTransformConfig> {

  @Override
  public String getIdentifier() {
    return Constants.TransformConstants.GENERATED_FILED_TRANSFORM;
  }

  @Override
  public GeneratedFieldTransformConfig getTransformConfig() {
    return new GeneratedFieldTransformConfig();
  }

  @Override
  public GeneratedFieldTransform getTransform(TransformConfig<?> transformConfig) {
    return new GeneratedFieldTransform((GeneratedFieldTransformConfig) transformConfig);
  }
}
