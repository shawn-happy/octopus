package com.octopus.kettlex.core.provider;

import com.octopus.kettlex.core.steps.Transform;
import com.octopus.kettlex.core.steps.config.TransformationConfig;

public interface TransformProvider<S extends Transform<?>, C extends TransformationConfig<?>>
    extends StepProvider {

  Class<S> getTransform();

  Class<C> getTransformConfig();
}
