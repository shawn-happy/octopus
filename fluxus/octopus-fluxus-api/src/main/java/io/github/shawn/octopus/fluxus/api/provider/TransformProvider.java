package io.github.shawn.octopus.fluxus.api.provider;

import io.github.shawn.octopus.fluxus.api.config.TransformConfig;
import io.github.shawn.octopus.fluxus.api.connector.Transform;

public interface TransformProvider<P extends TransformConfig<?>> {
  String getIdentifier();

  P getTransformConfig();

  Transform<P> getTransform(TransformConfig<?> transformConfig);
}
