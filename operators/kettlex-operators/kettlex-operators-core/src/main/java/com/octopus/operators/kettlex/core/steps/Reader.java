package com.octopus.operators.kettlex.core.steps;

import com.octopus.operators.kettlex.core.exception.KettleXStepExecuteException;
import com.octopus.operators.kettlex.core.steps.config.ReaderConfig;

public interface Reader<CONFIG extends ReaderConfig<?>> extends Step<CONFIG> {

  void read() throws KettleXStepExecuteException;
}
