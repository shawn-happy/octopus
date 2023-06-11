package com.octopus.kettlex.core.steps;

import com.octopus.kettlex.core.exception.KettleXStepExecuteException;
import com.octopus.kettlex.core.steps.config.TransformationConfig;

public interface Transform<CONFIG extends TransformationConfig<?>> extends Step<CONFIG> {

  void processRow() throws KettleXStepExecuteException;
}
