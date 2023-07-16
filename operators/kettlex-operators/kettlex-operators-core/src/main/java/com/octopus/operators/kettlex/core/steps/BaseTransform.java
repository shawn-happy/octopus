package com.octopus.operators.kettlex.core.steps;

import com.octopus.operators.kettlex.core.exception.KettleXStepExecuteException;
import com.octopus.operators.kettlex.core.management.ExecutionStatus;
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
      while ((record = getRow()) != null) {
        if (record instanceof TerminateRecord) {
          getCommunication().markStatus(ExecutionStatus.SUCCEEDED);
          break;
        }
        Record newRecord = processRecord(record);
        getCommunication().increaseTransformRecords(1);
        putRow(newRecord);
      }
      putRow(TerminateRecord.get());
    } catch (Exception e) {
      setError(e);
    }
  }

  protected abstract Record processRecord(Record record) throws KettleXStepExecuteException;
}
