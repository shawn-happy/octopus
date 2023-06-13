package com.octopus.kettlex.core.steps;

import com.octopus.kettlex.core.exception.KettleXStepExecuteException;
import com.octopus.kettlex.core.steps.config.TransformerConfig;

public interface Transform<CONFIG extends TransformerConfig<?>> extends Step<CONFIG> {

  void processRow() throws KettleXStepExecuteException;
}
