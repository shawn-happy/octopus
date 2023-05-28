package com.octopus.kettlex.core.steps;

import com.octopus.kettlex.core.exception.KettleXStepExecuteException;
import com.octopus.kettlex.core.row.RecordExchanger;

public interface Transform<CONFIG extends StepConfig, CONTEXT extends StepContext>
    extends Step<CONFIG, CONTEXT> {

  void processRow(RecordExchanger recordExchanger) throws KettleXStepExecuteException;
}
