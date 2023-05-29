package com.octopus.kettlex.core.steps;

import com.octopus.kettlex.core.exception.KettleXStepExecuteException;
import com.octopus.kettlex.core.row.RecordExchanger;

public interface Writer<CONFIG extends StepMeta> extends Step<CONFIG> {

  void writer(RecordExchanger recordExchanger) throws KettleXStepExecuteException;
}
