package com.octopus.kettlex.core.steps;

import com.octopus.kettlex.core.exception.KettleXStepExecuteException;
import com.octopus.kettlex.core.row.RecordExchanger;

public interface Transform<CONFIG extends StepMeta> extends Step<CONFIG> {

  void processRow(RecordExchanger recordExchanger) throws KettleXStepExecuteException;
}
