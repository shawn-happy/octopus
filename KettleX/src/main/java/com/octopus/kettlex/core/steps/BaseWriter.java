package com.octopus.kettlex.core.steps;

import com.octopus.kettlex.core.exception.KettleXStepExecuteException;
import com.octopus.kettlex.core.row.Record;
import com.octopus.kettlex.model.WriterConfig;
import com.octopus.kettlex.runtime.StepConfigChannelCombination;
import java.util.function.Consumer;

public abstract class BaseWriter<C extends WriterConfig<?>> extends BaseStep<C>
    implements Writer<C> {

  protected BaseWriter(StepConfigChannelCombination combination) {
    super(combination);
  }

  @Override
  public void writer() throws KettleXStepExecuteException {
    if (isShutdown()) {
      throw new KettleXStepExecuteException("step is shutdown");
    }
    Record record;
    while ((record = getRow()) != null) {
      doWriter().accept(record);
    }
  }

  protected abstract Consumer<Record> doWriter();
}
