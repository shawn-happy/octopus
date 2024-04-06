package io.github.shawn.octopus.fluxus.engine.connector.transform.valueMapper;

import io.github.shawn.octopus.fluxus.api.config.TransformConfig;
import io.github.shawn.octopus.fluxus.api.connector.Transform;
import io.github.shawn.octopus.fluxus.api.exception.DataWorkflowException;
import io.github.shawn.octopus.fluxus.api.provider.TransformProvider;

public class MapTransformProvider implements TransformProvider<MapTransformConfig> {
  @Override
  public String getIdentifier() {
    return "valueMapper";
  }

  @Override
  public MapTransformConfig getTransformConfig() {
    return new MapTransformConfig();
  }

  @Override
  public Transform<MapTransformConfig> getTransform(TransformConfig<?> transformConfig) {
    if (!(transformConfig instanceof MapTransformConfig)) {
      throw new DataWorkflowException("transform config type is not valueMapper");
    }
    return new MapTransform((MapTransformConfig) transformConfig);
  }
}
