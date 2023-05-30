package com.octopus.kettlex.core.steps;

import com.octopus.kettlex.core.exception.KettleXStepExecuteException;
import com.octopus.kettlex.core.row.RecordExchanger;
import com.octopus.kettlex.model.TransformationConfig;

public interface Transform<CONFIG extends TransformationConfig<?>> extends Step<CONFIG> {

  void processRow(RecordExchanger recordExchanger) throws KettleXStepExecuteException;
}
