package com.octopus.kettlex.core.steps;

import com.octopus.kettlex.core.exception.KettleXStepExecuteException;
import com.octopus.kettlex.core.row.Record;
import com.octopus.kettlex.core.steps.config.TransformerConfig;

public abstract class BaseTransform<C extends TransformerConfig<?>> extends BaseStep<C>
    implements Transform<C> {

  protected BaseTransform() {
    super();
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
