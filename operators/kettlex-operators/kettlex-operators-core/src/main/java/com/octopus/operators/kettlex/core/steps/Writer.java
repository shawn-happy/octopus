package com.octopus.operators.kettlex.core.steps;

import com.octopus.operators.kettlex.core.exception.KettleXStepExecuteException;
import com.octopus.operators.kettlex.core.steps.config.WriterConfig;

public interface Writer<CONFIG extends WriterConfig<?>> extends Step<CONFIG> {

  void writer() throws KettleXStepExecuteException;
}
