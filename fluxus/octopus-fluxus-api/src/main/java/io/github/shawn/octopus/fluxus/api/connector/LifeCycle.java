package io.github.shawn.octopus.fluxus.api.connector;

import io.github.shawn.octopus.fluxus.api.exception.StepExecutionException;

interface LifeCycle {

  default void init() throws StepExecutionException {}

  default void begin() throws StepExecutionException {}

  default void dispose() throws StepExecutionException {}
}
