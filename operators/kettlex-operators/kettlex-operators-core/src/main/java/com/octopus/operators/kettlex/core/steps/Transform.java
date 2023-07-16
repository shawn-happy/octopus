package com.octopus.operators.kettlex.core.steps;

import com.octopus.operators.kettlex.core.exception.KettleXStepExecuteException;
import com.octopus.operators.kettlex.core.steps.config.TransformerConfig;

public interface Transform<CONFIG extends TransformerConfig<?>> extends Step<CONFIG> {

  void processRow() throws KettleXStepExecuteException;
}
