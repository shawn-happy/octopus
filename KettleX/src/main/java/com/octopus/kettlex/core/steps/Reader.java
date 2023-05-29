package com.octopus.kettlex.core.steps;

import com.octopus.kettlex.core.exception.KettleXStepExecuteException;
import com.octopus.kettlex.core.row.RecordExchanger;

public interface Reader<CONFIG extends StepMeta> extends Step<CONFIG> {

  void read(RecordExchanger recordExchanger) throws KettleXStepExecuteException;
}
