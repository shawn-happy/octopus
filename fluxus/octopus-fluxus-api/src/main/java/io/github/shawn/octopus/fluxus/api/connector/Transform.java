package io.github.shawn.octopus.fluxus.api.connector;

import io.github.shawn.octopus.fluxus.api.config.TransformConfig;
import io.github.shawn.octopus.fluxus.api.exception.StepExecutionException;
import io.github.shawn.octopus.fluxus.api.model.table.RowRecord;

public interface Transform<P extends TransformConfig<?>> extends LifeCycle {

  RowRecord transform(RowRecord source) throws StepExecutionException;

  P getTransformConfig();
}
