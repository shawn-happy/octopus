package com.octopus.operators.kettlex.core.steps;

import com.octopus.operators.kettlex.core.exception.KettleXStepExecuteException;
import com.octopus.operators.kettlex.core.row.Record;
import com.octopus.operators.kettlex.core.row.record.TerminateRecord;
import com.octopus.operators.kettlex.core.steps.config.TransformerConfig;

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
    try {
      stepListeners.forEach(stepListener -> stepListener.onRunnable(stepContext));
      while ((record = getRow()) != null) {
        if (record instanceof TerminateRecord) {
          break;
        }
        Record newRecord = processRecord(record);
        putRow(newRecord);
        stepContext.getCommunication().increaseTransformRecords(1);
      }
      putRow(TerminateRecord.get());
      stepListeners.forEach(stepListener -> stepListener.onSuccess(stepContext));
    } catch (Exception e) {
      setError(e);
      throw new KettleXStepExecuteException(
          String.format("%s transfer error", stepContext.getStepName()), e);
    }
  }

  protected abstract Record processRecord(Record record) throws KettleXStepExecuteException;
}
