package com.octopus.kettlex.core.steps;

import com.octopus.kettlex.core.exception.KettleXStepExecuteException;
import com.octopus.kettlex.core.row.Record;
import com.octopus.kettlex.model.TransformationConfig;

public abstract class BaseTransform<C extends TransformationConfig<?>> extends BaseStep<C>
    implements Transform<C> {

  protected BaseTransform(C stepConfig) {
    super(stepConfig);
  }

  @Override
  public void processRow() throws KettleXStepExecuteException {
    Record record;
    while ((record = getRow()) != null) {
      Record newRecord = processRecord(record);
      putRow(newRecord);
    }
  }

  protected abstract Record processRecord(Record record);
}
