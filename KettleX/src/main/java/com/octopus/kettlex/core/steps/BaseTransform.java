package com.octopus.kettlex.core.steps;

import com.octopus.kettlex.core.exception.KettleXStepExecuteException;
import com.octopus.kettlex.core.row.Record;
import com.octopus.kettlex.model.TransformationConfig;
import com.octopus.kettlex.runtime.StepConfigChannelCombination;

public abstract class BaseTransform<C extends TransformationConfig<?>> extends BaseStep<C>
    implements Transform<C> {

  protected BaseTransform(StepConfigChannelCombination combination) {
    super(combination);
  }

  @Override
  public void processRow() throws KettleXStepExecuteException {
    if (isShutdown()) {
      throw new KettleXStepExecuteException("step is shutdown");
    }
    Record record;
    while ((record = getRow()) != null) {
      Record newRecord = processRecord(record);
      putRow(newRecord);
    }
  }

  protected abstract Record processRecord(Record record);
}
