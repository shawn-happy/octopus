package com.octopus.kettlex.core.provider;

import com.octopus.kettlex.core.steps.Transform;
import com.octopus.kettlex.core.steps.config.TransformerConfig;

public interface TransformProvider<S extends Transform<?>, C extends TransformerConfig<?>>
    extends StepProvider {

  Class<S> getTransform();

  Class<C> getTransformConfig();
}
