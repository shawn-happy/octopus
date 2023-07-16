package com.octopus.operators.kettlex.core.provider;

import com.octopus.operators.kettlex.core.steps.Transform;
import com.octopus.operators.kettlex.core.steps.config.TransformerConfig;

public interface TransformProvider<S extends Transform<?>, C extends TransformerConfig<?>>
    extends StepProvider {

  Class<S> getTransform();

  Class<C> getTransformConfig();
}
